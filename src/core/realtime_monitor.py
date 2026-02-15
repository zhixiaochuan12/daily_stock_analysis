# -*- coding: utf-8 -*-
"""
===================================
实时股票监控模块
===================================

职责：
1. 定期扫描热门板块，识别涨跌幅TopK股票
2. 自动触发分析流程（新闻搜索 + AI分析 + 推送）
3. 交易时段检测和去重机制
4. 复用现有分析流水线
"""

import logging
import signal
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Any, Optional
from threading import Lock

from src.config import Config
from src.core.pipeline import StockAnalysisPipeline
from src.enums import ReportType
from data_provider import DataFetcherManager
from src.scheduler import GracefulShutdown

logger = logging.getLogger(__name__)


class RealtimeStockMonitor:
    """
    实时股票监控器
    
    功能：
    1. 每N分钟扫描一次热门板块
    2. 获取板块成分股，筛选涨跌幅TopK
    3. 触发分析流程（复用 StockAnalysisPipeline）
    4. 交易时段检测和去重机制
    """
    
    def __init__(self, config: Config):
        """
        初始化监控器
        
        Args:
            config: 配置对象
        """
        self.config = config
        self.fetcher_manager = DataFetcherManager()
        
        # 去重缓存：记录已分析的股票和时间戳
        # 格式: {stock_code: timestamp}
        self._analyzed_cache: Dict[str, float] = {}
        self._cache_lock = Lock()
        self._cache_ttl = 3600  # 1小时TTL
        
        # 优雅退出处理器
        self.shutdown_handler = GracefulShutdown()
        
        logger.info("实时监控器初始化完成")
        logger.info(f"监控间隔: {config.realtime_monitor_interval} 分钟")
        logger.info(f"TopK: {config.realtime_monitor_topk}")
        logger.info(f"热门板块数: {config.realtime_monitor_hot_sectors_num}")
        logger.info(f"监控类型: {config.realtime_monitor_type}")
        logger.info(f"仅交易时段: {config.realtime_monitor_trading_hours_only}")
        logger.info(f"最小涨跌幅阈值: {config.realtime_monitor_min_change_pct}%")
        if config.debug:
            logger.info("⚠️  Debug 模式已启用：允许非交易时段运行（测试模式）")
    
    def is_trading_hours(self) -> bool:
        """
        检查当前是否在交易时段
        
        Returns:
            True if in trading hours, False otherwise
        """
        # Debug 模式下自动允许非交易时段运行（便于测试）
        if self.config.debug:
            logger.debug("[测试模式] Debug 模式已启用，允许非交易时段运行")
            return True
        
        if not self.config.realtime_monitor_trading_hours_only:
            return True
        
        # 使用中国时区 (UTC+8)
        tz_cn = timezone(timedelta(hours=8))
        now = datetime.now(tz_cn)
        weekday = now.weekday()  # 0=Monday, 6=Sunday
        
        # 周末不交易
        if weekday >= 5:
            return False
        
        # A股交易时段: 9:30-11:30, 13:00-15:00
        # 港股交易时段: 9:30-12:00, 13:00-16:00
        # 这里统一使用 A股时段 (9:30-15:00)
        hour = now.hour
        minute = now.minute
        current_time = hour * 60 + minute
        
        # 上午: 9:30-11:30
        morning_start = 9 * 60 + 30  # 9:30
        morning_end = 11 * 60 + 30   # 11:30
        
        # 下午: 13:00-15:00
        afternoon_start = 13 * 60    # 13:00
        afternoon_end = 15 * 60      # 15:00
        
        return (morning_start <= current_time <= morning_end) or \
               (afternoon_start <= current_time <= afternoon_end)
    
    def _cleanup_cache(self):
        """清理过期的缓存条目"""
        current_time = time.time()
        with self._cache_lock:
            expired_keys = [
                code for code, timestamp in self._analyzed_cache.items()
                if current_time - timestamp > self._cache_ttl
            ]
            for key in expired_keys:
                del self._analyzed_cache[key]
            if expired_keys:
                logger.debug(f"清理了 {len(expired_keys)} 个过期缓存条目")
    
    def _is_recently_analyzed(self, stock_code: str) -> bool:
        """
        检查股票是否在最近分析过
        
        Args:
            stock_code: 股票代码
            
        Returns:
            True if analyzed recently, False otherwise
        """
        self._cleanup_cache()
        current_time = time.time()
        
        with self._cache_lock:
            if stock_code in self._analyzed_cache:
                age = current_time - self._analyzed_cache[stock_code]
                if age < self._cache_ttl:
                    logger.debug(f"[去重] {stock_code} 在 {int(age/60)} 分钟前已分析，跳过")
                    return True
                else:
                    # 过期，删除
                    del self._analyzed_cache[stock_code]
            return False
    
    def _mark_analyzed(self, stock_code: str):
        """标记股票为已分析"""
        with self._cache_lock:
            self._analyzed_cache[stock_code] = time.time()
    
    def _get_hot_sectors(self) -> List[Dict[str, Any]]:
        """
        获取热门板块（涨幅/跌幅前N）
        
        Returns:
            板块列表，每个包含 name 和 change_pct
        """
        try:
            n = self.config.realtime_monitor_hot_sectors_num
            top_sectors, bottom_sectors = self.fetcher_manager.get_sector_rankings(n)
            
            result = []
            
            # 根据监控类型添加板块
            if self.config.realtime_monitor_type in ('gainers', 'both'):
                result.extend(top_sectors)
            
            if self.config.realtime_monitor_type in ('losers', 'both'):
                result.extend(bottom_sectors)
            
            logger.info(f"获取到 {len(result)} 个热门板块")
            for sector in result:
                logger.info(f"  - {sector.get('name')}: {sector.get('change_pct', 0):.2f}%")
            
            return result
            
        except Exception as e:
            logger.error(f"获取热门板块失败: {e}")
            return []
    
    def _get_stocks_from_sectors(self, sectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        从热门板块获取成分股
        
        Args:
            sectors: 板块列表
            
        Returns:
            股票列表，每个包含 code, name, change_pct, sector
        """
        all_stocks = []
        
        # 获取 akshare_fetcher 实例
        akshare_fetcher = None
        for fetcher in self.fetcher_manager._fetchers:
            if hasattr(fetcher, 'get_sector_constituent_stocks'):
                akshare_fetcher = fetcher
                break
        
        if not akshare_fetcher:
            logger.error("未找到支持获取板块成分股的数据源")
            return []
        
        # 优化：先获取一次板块列表，避免每个板块都调用一次
        import akshare as ak
        sectors_df = None
        try:
            logger.info("[API调用] 一次性获取板块列表（供多个板块复用）...")
            sectors_df = ak.stock_board_industry_name_em()
            logger.info(f"[优化] 已获取板块列表，共 {len(sectors_df) if sectors_df is not None else 0} 个板块")
        except Exception as e:
            logger.warning(f"获取板块列表失败，将逐个获取: {e}")
        
        for sector in sectors:
            sector_name = sector.get('name', '')
            if not sector_name:
                continue
            
            try:
                # 传递已获取的板块列表，避免重复调用
                stocks = akshare_fetcher.get_sector_constituent_stocks(sector_name, sectors_df=sectors_df)
                for stock in stocks:
                    stock['sector'] = sector_name
                    stock['sector_change_pct'] = sector.get('change_pct', 0)
                all_stocks.extend(stocks)
                logger.debug(f"板块 '{sector_name}' 获取到 {len(stocks)} 只股票")
            except Exception as e:
                logger.warning(f"获取板块 '{sector_name}' 成分股失败: {e}")
                continue
        
        return all_stocks
    
    def _select_topk_stocks(self, stocks: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        选择涨跌幅TopK股票
        
        Args:
            stocks: 股票列表
            
        Returns:
            TopK股票列表（涨幅TopK + 跌幅TopK）
        """
        if not stocks:
            return []
        
        # 过滤：只保留满足最小涨跌幅阈值的股票
        min_change = self.config.realtime_monitor_min_change_pct
        filtered = [
            s for s in stocks
            if s.get('change_pct') is not None and abs(s.get('change_pct', 0)) >= min_change
        ]
        
        logger.info(f"过滤后剩余 {len(filtered)} 只股票（阈值: {min_change}%）")
        
        # 去重（按代码）
        seen = set()
        unique_stocks = []
        for stock in filtered:
            code = stock.get('code', '')
            if code and code not in seen:
                seen.add(code)
                unique_stocks.append(stock)
        
        # 按涨跌幅排序
        unique_stocks.sort(key=lambda x: x.get('change_pct', 0), reverse=True)
        
        # 选择TopK
        topk = self.config.realtime_monitor_topk
        
        # 根据监控类型选择
        if self.config.realtime_monitor_type == 'gainers':
            # 只选涨幅TopK
            selected = unique_stocks[:topk]
        elif self.config.realtime_monitor_type == 'losers':
            # 只选跌幅TopK（取最后topk个）
            selected = unique_stocks[-topk:] if len(unique_stocks) >= topk else unique_stocks
        else:  # both
            # 涨幅TopK + 跌幅TopK
            top_gainers = unique_stocks[:topk]
            top_losers = unique_stocks[-topk:] if len(unique_stocks) >= topk else []
            # 合并并去重
            selected = top_gainers + [s for s in top_losers if s not in top_gainers]
        
        logger.info(f"选择 {len(selected)} 只股票进行分析")
        for stock in selected:
            logger.info(f"  - {stock.get('code')} {stock.get('name')}: {stock.get('change_pct', 0):.2f}% "
                       f"(板块: {stock.get('sector', 'N/A')})")
        
        return selected
    
    def _run_monitoring_cycle(self):
        """执行一次监控周期"""
        try:
            logger.info("=" * 60)
            logger.info(f"开始实时监控周期 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.info("=" * 60)
            
            # 1. 检查交易时段
            if not self.is_trading_hours():
                if self.config.debug:
                    logger.info("⚠️  [测试模式] 当前不在交易时段，但 Debug 模式已启用，继续执行")
                else:
                    logger.info("当前不在交易时段，跳过监控")
                    return
            
            # 2. 获取热门板块
            sectors = self._get_hot_sectors()
            if not sectors:
                logger.warning("未获取到热门板块，跳过本次监控")
                return
            
            # 3. 获取板块成分股
            all_stocks = self._get_stocks_from_sectors(sectors)
            if not all_stocks:
                logger.warning("未获取到成分股，跳过本次监控")
                return
            
            # 4. 选择TopK股票
            selected_stocks = self._select_topk_stocks(all_stocks)
            if not selected_stocks:
                logger.info("未筛选到符合条件的股票")
                return
            
            # 5. 去重：过滤最近分析过的股票
            new_stocks = []
            for stock in selected_stocks:
                code = stock.get('code', '')
                if code and not self._is_recently_analyzed(code):
                    new_stocks.append(stock)
                else:
                    logger.debug(f"跳过已分析的股票: {code}")
            
            if not new_stocks:
                logger.info("所有股票都在最近分析过，跳过本次分析")
                return
            
            logger.info(f"准备分析 {len(new_stocks)} 只新股票")
            
            # 6. 触发分析流程
            query_id = f"realtime_{int(time.time())}"
            pipeline = StockAnalysisPipeline(
                config=self.config,
                max_workers=min(self.config.max_workers, len(new_stocks)),
                query_id=query_id,
                query_source="realtime_monitor"
            )
            
            # 逐个分析股票
            for stock in new_stocks:
                code = stock.get('code', '')
                if not code:
                    continue
                
                try:
                    logger.info(f"[监控触发] 开始分析 {code} ({stock.get('name', 'N/A')})")
                    
                    # 执行分析（启用单股推送）
                    result = pipeline.process_single_stock(
                        code=code,
                        skip_analysis=False,
                        single_stock_notify=True,
                        report_type=ReportType.SIMPLE
                    )
                    
                    if result:
                        # 标记为已分析
                        self._mark_analyzed(code)
                        logger.info(f"[监控触发] {code} 分析完成")
                    else:
                        logger.warning(f"[监控触发] {code} 分析返回空结果")
                    
                    # 避免API限流，添加短暂延迟
                    time.sleep(2)
                    
                except Exception as e:
                    logger.exception(f"[监控触发] 分析 {code} 失败: {e}")
                    continue
            
            logger.info(f"监控周期完成，共分析 {len(new_stocks)} 只股票")
            
        except Exception as e:
            logger.exception(f"监控周期执行失败: {e}")
    
    def run(self):
        """
        启动监控器（阻塞运行）
        
        使用 schedule 库实现周期性任务
        """
        try:
            import schedule
        except ImportError:
            logger.error("schedule 库未安装，请执行: pip install schedule")
            raise ImportError("请安装 schedule 库: pip install schedule")
        
        # 设置周期性任务
        interval = self.config.realtime_monitor_interval
        schedule.every(interval).minutes.do(self._run_monitoring_cycle)
        
        logger.info("=" * 60)
        logger.info("实时监控器启动")
        logger.info(f"监控间隔: 每 {interval} 分钟")
        logger.info(f"下次执行: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("=" * 60)
        
        # 立即执行一次（如果当前是交易时段）
        if self.is_trading_hours():
            logger.info("当前在交易时段，立即执行一次监控")
            self._run_monitoring_cycle()
        else:
            logger.info("当前不在交易时段，等待下次执行")
        
        # 主循环
        while not self.shutdown_handler.should_shutdown:
            schedule.run_pending()
            time.sleep(30)  # 每30秒检查一次
            
            # 每小时打印一次心跳
            now = datetime.now()
            if now.minute == 0 and now.second < 30:
                next_run = schedule.next_run()
                if next_run:
                    logger.info(f"监控器运行中... 下次执行: {next_run.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("实时监控器已停止")
    
    def stop(self):
        """停止监控器"""
        # Trigger shutdown by setting the flag directly
        with self.shutdown_handler._lock:
            self.shutdown_handler.shutdown_requested = True
