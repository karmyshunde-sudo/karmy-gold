#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置模块
提供项目全局配置参数，包括路径、日志、策略参数等
特别优化了时区相关配置，确保所有时间显示为北京时间
"""

import os
import logging
import sys
from typing import Dict, Any, Optional
from datetime import datetime, timezone, timedelta

# 获取项目根目录路径
def _get_base_dir() -> str:
    """获取项目根目录路径"""
    try:
        # 优先使用GITHUB_WORKSPACE环境变量（GitHub Actions环境）
        base_dir = os.environ.get('GITHUB_WORKSPACE')
        if base_dir and os.path.exists(base_dir):
            return os.path.abspath(base_dir)
        
        # 尝试基于当前文件位置计算项目根目录
        current_file_path = os.path.abspath(__file__)
        base_dir = os.path.dirname(current_file_path)
        
        # 确保目录存在
        if os.path.exists(base_dir):
            return os.path.abspath(base_dir)
        
        # 作为最后手段，使用当前工作目录
        return os.path.abspath(os.getcwd())
    except Exception as e:
        print(f"获取项目根目录失败: {str(e)}", file=sys.stderr)
        # 退回到当前工作目录
        return os.path.abspath(os.getcwd())

class Config:
    """
    全局配置类：数据源配置、策略参数、文件路径管理
    所有配置项均有默认值，并支持从环境变量覆盖
    """
    
    # -------------------------
    # 0. 时区定义
    # -------------------------
    # 严格遵守要求：在config.py中定义两个变量，分别保存平台时间UTC，北京时间UTC+8
    UTC_TIMEZONE = timezone.utc
    BEIJING_TIMEZONE = timezone(timedelta(hours=8))
    
    # -------------------------
    # 1. 数据源配置
    # -------------------------
    # 初次爬取默认时间范围（1年）
    INITIAL_CRAWL_DAYS: int = 365

    # ETF列表更新间隔（天）
    ETF_LIST_UPDATE_INTERVAL: int = 7  
    # 每7天更新一次ETF列表
    
    # 英文列名到中文列名的映射
    COLUMN_NAME_MAPPING: Dict[str, str] = {
        "date": "日期",
        "open": "开盘",
        "close": "收盘",
        "high": "最高",
        "low": "最低",
        "volume": "成交量",
        "amount": "成交额",
        "amplitude": "振幅",
        "pct_change": "涨跌幅",
        "price_change": "涨跌额",
        "turnover": "换手率",
        "etf_code": "ETF代码",
        "etf_name": "ETF名称",
        "crawl_time": "爬取时间",
        "index_close": "指数收盘",  # 新增：指数收盘价
        "tracking_error": "跟踪误差",  # 新增：跟踪误差
        "bid_ask_spread": "买卖价差",  # 新增：买卖价差
        "bid_volume": "买一量",  # 新增：买一量
        "ask_volume": "卖一量"  # 新增：卖一量
    }
    
    # 标准列名（中文）
    STANDARD_COLUMNS: list = list(COLUMN_NAME_MAPPING.values())
    
    # ETF列表标准列（确保all_etfs.csv和karmy_etf.csv结构一致）
    ETF_STANDARD_COLUMNS: list = ["ETF代码", "ETF名称", "完整代码", "基金规模", "成立日期"]
    
    # 新浪数据源备用接口
    SINA_ETF_HIST_URL: str = "https://finance.sina.com.cn/realstock/company/{etf_code}/hisdata/klc_kl.js"
    
    # 批量爬取批次大小
    CRAWL_BATCH_SIZE: int = 50  # 每批50只ETF

    # -------------------------
    # 2. 策略参数配置
    # -------------------------
    # 基础评分权重（会根据市场环境动态调整）
    BASE_SCORE_WEIGHTS: Dict[str, float] = {
        'liquidity': 0.15,   # 流动性评分权重
        'risk': 0.25,        # 风险控制评分权重
        'return': 0.20,      # 收益能力评分权重
        'tracking': 0.20,    # 跟踪能力评分权重
        'premium': 0.10,     # 溢价率评分权重
        'stability': 0.10    # 规模稳定性评分权重
    }
    
    # 市场环境定义阈值
    MARKET_CONDITION_THRESHOLDS: Dict[str, float] = {
        'bull_short_trend': 0.05,  # 牛市短期趋势阈值
        'bull_mid_trend': 0.03,    # 牛市中期趋势阈值
        'bull_momentum': 0.05,     # 牛市动量阈值
        'bear_short_trend': -0.05, # 熊市短期趋势阈值
        'bear_mid_trend': -0.03,   # 熊市中期趋势阈值
        'bear_momentum': -0.05,    # 熊市动量阈值
        'sideways_volatility': 0.2,  # 震荡市波动率阈值
        'sideways_trend': 0.03      # 震荡市趋势阈值
    }
    
    # 套利策略：交易成本（印花税0.1%+佣金0.02%）
    TRADE_COST_RATE: float = 0.0012  # 0.12%
    
    # 套利阈值（收益率超过该值才推送）
    ARBITRAGE_PROFIT_THRESHOLD: float = 0.005  # 0.5%
    
    # 综合评分筛选阈值（仅保留评分前N%的ETF）
    SCORE_TOP_PERCENT: int = 20  # 保留前20%高分ETF
    
    # 仓位策略参数（均线策略）
    MA_SHORT_PERIOD: int = 5    # 短期均线（5日）
    MA_LONG_PERIOD: int = 20    # 长期均线（20日）
    ADD_POSITION_THRESHOLD: float = 0.03  # 加仓阈值（涨幅超3%）
    STOP_LOSS_THRESHOLD: float = -0.05    # 止损阈值（跌幅超5%")
    
    # 评分维度权重
    SCORE_WEIGHTS: Dict[str, float] = {
        'liquidity': 0.20,  # 流动性评分权重
        'risk': 0.25,       # 风险控制评分权重
        'return': 0.25,     # 收益能力评分权重
        'premium': 0.15,    # 溢价率评分权重
        'sentiment': 0.15   # 情绪指标评分权重
    }
    
    # 仓位策略参数 - 优化版
    STRATEGY_PARAMETERS = {
        "稳健仓": {
            "min_fund_size": 10.0,  # 基金规模≥10亿元
            "min_avg_volume": 5000.0,  # 日均成交额≥5000万元
            "max_holdings": 4,  # 最大持有ETF数量
            "min_correlation": 0.7,  # 最小相关性阈值
            "ma_period": 20,  # 均线周期
            "confirm_days": 3,  # 信号确认天数
            "initial_position": 0.3,  # 初始仓位比例(30%)
            "add_position": [0.2, 0.1],  # 加仓比例(20%, 10%)
            "stop_loss_ratio": 0.05,  # 动态止损比例(5%)
            "max_position": 0.7,  # 最大仓位比例(70%)
            "max_drawdown_warning": 0.10,  # 最大回撤预警阈值
            "risk_level": "low"  # 风险级别
        },
        "激进仓": {
            "min_fund_size": 2.0,  # 基金规模≥2亿元
            "min_avg_volume": 1000.0,  # 日均成交额≥1000万元
            "max_holdings": 3,  # 最大持有ETF数量
            "min_correlation": 0.8,  # 最小相关性阈值
            "ma_period": 20,  # 均线周期
            "confirm_days": 2,  # 信号确认天数
            "initial_position": 0.2,  # 初始仓位比例(20%)
            "add_position": [0.15],  # 加仓比例(15%)
            "stop_loss_ratio": 0.08,  # 动态止损比例(8%)
            "max_position": 0.6,  # 最大仓位比例(60%)
            "max_drawdown_warning": 0.15,  # 最大回撤预警阈值
            "risk_level": "medium"  # 风险级别
        },
        "套利仓": {
            "min_fund_size": 5.0,  # 基金规模≥5亿元
            "min_avg_volume": 3000.0,  # 日均成交额≥3000万元
            "max_holdings": 5,  # 最大持有ETF数量
            "min_correlation": 0.6,  # 最小相关性阈值
            "ma_period": 10,  # 均线周期
            "confirm_days": 1,  # 信号确认天数
            "initial_position": 0.1,  # 初始仓位比例(10%)
            "add_position": [0.05],  # 加仓比例(5%)
            "stop_loss_ratio": 0.02,  # 动态止损比例(2%)
            "max_position": 0.3,  # 最大仓位比例(30%)
            "max_drawdown_warning": 0.05,  # 最大回撤预警阈值
            "risk_level": "high"  # 风险级别
        }
    }
    
    # 买入信号条件
    BUY_SIGNAL_DAYS: int = 2  # 连续几天信号持续才买入
    
    # 换股条件
    SWITCH_THRESHOLD: float = 0.3  # 新ETF比原ETF综合评分高出30%则换股

    # -------------------------
    # 3. 文件路径配置 - 基于仓库根目录的路径
    # -------------------------
    BASE_DIR: str = _get_base_dir()
    
    # 数据存储路径
    DATA_DIR: str = os.path.join(BASE_DIR, "data")
    ETFS_DAILY_DIR: str = os.path.join(DATA_DIR, "etf_daily")
    
    # ETF元数据（记录最后爬取日期）
    METADATA_PATH: str = os.path.join(DATA_DIR, "etf_metadata.csv")
    
    # ETF评分历史数据
    SCORE_HISTORY_PATH: str = os.path.join(DATA_DIR, "etf_score_history.csv")
    
    # ETF相关性数据
    CORRELATION_DATA_PATH: str = os.path.join(DATA_DIR, "etf_correlation_data.csv")
    
    # 策略结果标记（避免单日重复推送）
    FLAG_DIR: str = os.path.join(DATA_DIR, "flags")
    
    # 套利结果标记文件
    @staticmethod
    def get_arbitrage_flag_file(date_str: Optional[str] = None) -> str:
        """获取套利标记文件路径"""
        try:
            from date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
        except ImportError:
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"arbitrage_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取套利标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "arbitrage_pushed_error.txt")
    
    # 仓位策略结果标记文件
    @staticmethod
    def get_position_flag_file(date_str: Optional[str] = None) -> str:
        """获取仓位标记文件路径"""
        try:
            from date_utils import get_beijing_time
            date = date_str or get_beijing_time().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"position_pushed_{date}.txt")
        except ImportError:
            date = date_str or datetime.now().strftime("%Y-%m-%d")
            return os.path.join(Config.FLAG_DIR, f"position_pushed_{date}.txt")
        except Exception as e:
            logging.error(f"获取仓位标记文件路径失败: {str(e)}", exc_info=True)
            return os.path.join(Config.FLAG_DIR, "position_pushed_error.txt")
    
    # 交易记录文件
    TRADE_RECORD_FILE: str = os.path.join(DATA_DIR, "trade_records.csv")
    
    # 仓位记录文件
    POSITION_RECORD_FILE: str = os.path.join(DATA_DIR, "position_records.csv")
    
    # 风险监控记录文件
    RISK_MONITOR_FILE: str = os.path.join(DATA_DIR, "risk_monitor_records.csv")
    
    # 全市场ETF列表存储路径
    ALL_ETFS_PATH: str = os.path.join(DATA_DIR, "all_etfs.csv")
    
    # 兜底ETF列表路径
    BACKUP_ETFS_PATH: str = os.path.join(BASE_DIR, "karmy-etf.csv")

    # -------------------------
    # 4. 日志配置
    # -------------------------
    @staticmethod
    def setup_logging(log_level: Optional[str] = None,
                     log_file: Optional[str] = None) -> None:
        """
        配置日志系统
        :param log_level: 日志级别 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        :param log_file: 日志文件路径，如果为None则只输出到控制台
        """
        try:
            level = log_level or Config.LOG_LEVEL
            log_format = Config.LOG_FORMAT
            
            # 创建根日志记录器
            root_logger = logging.getLogger()
            root_logger.setLevel(level)
            
            # 清除现有处理器
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # 创建格式化器
            formatter = logging.Formatter(log_format)
            
            # 创建控制台处理器
            console_handler = logging.StreamHandler()
            console_handler.setLevel(level)
            console_handler.setFormatter(formatter)
            root_logger.addHandler(console_handler)
            
            # 创建文件处理器（如果指定了日志文件）
            if log_file:
                try:
                    # 确保日志目录存在
                    log_dir = os.path.dirname(log_file)
                    if log_dir and not os.path.exists(log_dir):
                        os.makedirs(log_dir, exist_ok=True)
                    
                    file_handler = logging.FileHandler(log_file, encoding='utf-8')
                    file_handler.setLevel(level)
                    file_handler.setFormatter(formatter)
                    root_logger.addHandler(file_handler)
                    logging.info(f"日志文件已配置: {log_file}")
                except Exception as e:
                    logging.error(f"配置日志文件失败: {str(e)}", exc_info=True)
        except Exception as e:
            logging.error(f"配置日志系统失败: {str(e)}", exc_info=True)
    
    LOG_LEVEL: str = "INFO"
    LOG_FORMAT: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    LOG_DIR: str = os.path.join(BASE_DIR, "data", "logs")  # 日志目录配置
    LOG_FILE: str = os.path.join(LOG_DIR, "karmy_gold.log")  # 日志文件路径

    # -------------------------
    # 5. 新增：网络请求配置
    # -------------------------
    # 请求超时设置（秒）
    REQUEST_TIMEOUT: int = 30
    
    # -------------------------
    # 6. 企业微信机器人配置
    # 企业微信消息固定末尾（用于标识消息来源）
    # -------------------------
    # 直接作为类属性，确保其他模块能直接访问
    WECOM_WEBHOOK: str = os.getenv("WECOM_WEBHOOK", "")

    WECOM_MESFOOTER: str = (
        "\n\n"
        "【Karmy-Gold】\n"
        "📊 数据来源：AkShare | 环境：生产\n"
        "🕒 消息生成时间：{current_time}"
    )
    
    # -------------------------
    # 7. ETF筛选配置
    # -------------------------
    # ETF筛选参数 - 全球默认值
    GLOBAL_MIN_FUND_SIZE: float = 10.0  # 默认基金规模≥10亿元
    GLOBAL_MIN_AVG_VOLUME: float = 5000.0  # 默认日均成交额≥5000万元

    # ETF筛选参数 - 多层次筛选体系
    ETF_SELECTION_LAYERS = {
        "base": {  # 基础层（稳健仓使用）
            "min_fund_size": 10.0,      # 基金规模≥10亿元
            "min_avg_volume": 5000.0,   # 日均成交额≥5000万元
            "min_listing_days": 365,    # 成立时间≥1年
            "max_tracking_error": 0.05  # 最大跟踪误差
        },
        "opportunity": {  # 机会层（激进仓使用）
            "min_fund_size": 2.0,       # 基金规模≥2亿元
            "min_avg_volume": 1000.0,   # 日均成交额≥1000万元
            "min_listing_days": 180,    # 成立时间≥6个月
            "max_tracking_error": 0.08  # 最大跟踪误差
        },
        "growth": {  # 成长期（成长仓使用）
            "min_fund_size": 0.5,       # 基金规模≥0.5亿元
            "min_avg_volume": 300.0,    # 日均成交额≥300万元
            "max_listing_days": 180,    # 成立时间≤6个月
            "min_size_growth": 0.3      # 规模月增长率≥30%
        },
        "sector_special": {  # 行业特色层
            "min_fund_size": 3.0,       # 基金规模≥3亿元
            "min_avg_volume": 800.0,    # 日均成交额≥800万元
            "sector_focus": ["科技", "医药", "新能源"]  # 重点关注行业
        }
    }

    # -------------------------
    # 8. 路径初始化方法
    # -------------------------
    @staticmethod
    def init_dirs() -> bool:
        """
        初始化所有必要目录
        :return: 是否成功初始化所有目录
        """
        try:
            # 确保数据目录存在
            dirs_to_create = [
                Config.DATA_DIR,
                Config.ETFS_DAILY_DIR,
                Config.FLAG_DIR,
                Config.LOG_DIR,
                os.path.dirname(Config.TRADE_RECORD_FILE),
                os.path.dirname(Config.POSITION_RECORD_FILE),
                os.path.dirname(Config.RISK_MONITOR_FILE),
                os.path.dirname(Config.ALL_ETFS_PATH)
            ]
            
            for dir_path in dirs_to_create:
                if dir_path and not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)
                    logging.info(f"创建目录: {dir_path}")
            
            # 初始化日志
            Config.setup_logging(log_file=Config.LOG_FILE)
            
            return True
            
        except Exception as e:
            logging.error(f"初始化目录失败: {str(e)}", exc_info=True)
            return False

    # -------------------------
    # 9. 数据保留策略
    # -------------------------
    DATA_RETENTION_POLICY = {
        "etf_daily": 365,    # ETF日线数据保留天数（1年）
        "logs": 7,           # 日志文件保留天数（7天）
        "temp_files": 7      # 临时文件保留天数（7天）
    }

# -------------------------
# 初始化配置
# -------------------------
try:
    # 首先尝试初始化基础目录
    base_dir = _get_base_dir()
    
    # 重新定义关键路径，确保它们基于正确的base_dir
    Config.BASE_DIR = base_dir
    Config.DATA_DIR = os.path.join(base_dir, "data")
    Config.ETFS_DAILY_DIR = os.path.join(Config.DATA_DIR, "etf_daily")
    Config.FLAG_DIR = os.path.join(Config.DATA_DIR, "flags")
    Config.LOG_DIR = os.path.join(Config.DATA_DIR, "logs")
    Config.LOG_FILE = os.path.join(Config.LOG_DIR, "karmy_gold.log")
    
    # 设置基础日志配置
    logging.basicConfig(
        level=Config.LOG_LEVEL,
        format=Config.LOG_FORMAT,
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 初始化目录
    if Config.init_dirs():
        logging.info("配置初始化完成")
    else:
        logging.warning("配置初始化完成，但存在警告")
        
except Exception as e:
    # 创建一个临时的、基本的日志配置
    logging.basicConfig(
        level="INFO",
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    
    # 记录错误但继续执行
    logging.error(f"配置初始化失败: {str(e)}", exc_info=True)
    logging.info("已设置基础日志配置，继续执行")

# -------------------------
# 检查环境变量
# -------------------------
try:
    wecom_webhook = os.getenv("WECOM_WEBHOOK")
    if wecom_webhook:
        logging.info("检测到WECOM_WEBHOOK环境变量已设置")
    else:
        logging.warning("WECOM_WEBHOOK环境变量未设置，微信推送可能无法工作")
        
    # 确保Config中的WECOM_WEBHOOK与环境变量一致
    Config.WECOM_WEBHOOK = wecom_webhook or ""
except Exception as e:
    logging.error(f"检查环境变量时出错: {str(e)}", exc_info=True)