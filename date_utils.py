#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日期工具模块
提供日期、时间相关工具函数，特别优化了时区处理
确保所有时间显示为北京时间
"""

import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Tuple, Optional
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

def get_current_times() -> Tuple[datetime, datetime]:
    """
    获取当前UTC时间和北京时间
    
    Returns:
        Tuple[datetime, datetime]: (UTC时间, 北京时间)
    """
    try:
        # 获取UTC时间
        utc_now = datetime.now(timezone.utc)
        
        # 转换为北京时间
        beijing_now = utc_now.astimezone(Config.BEIJING_TIMEZONE)
        
        return utc_now, beijing_now
    
    except Exception as e:
        logger.error(f"获取当前时间失败: {str(e)}", exc_info=True)
        # 返回本地时间作为备用
        now = datetime.now()
        return now, now

def get_beijing_time() -> datetime:
    """
    获取当前北京时间
    
    Returns:
        datetime: 北京时间
    """
    try:
        utc_now = datetime.now(timezone.utc)
        return utc_now.astimezone(Config.BEIJING_TIMEZONE)
    
    except Exception as e:
        logger.error(f"获取北京时间失败: {str(e)}", exc_info=True)
        # 返回本地时间作为备用
        return datetime.now()

def get_utc_time() -> datetime:
    """
    获取当前UTC时间
    
    Returns:
        datetime: UTC时间
    """
    try:
        return datetime.now(timezone.utc)
    
    except Exception as e:
        logger.error(f"获取UTC时间失败: {str(e)}", exc_info=True)
        # 返回本地时间作为备用
        return datetime.now()

def is_file_outdated(file_path: str, max_age_days: int) -> bool:
    """
    检查文件是否过期
    
    Args:
        file_path: 文件路径
        max_age_days: 最大年龄（天）
    
    Returns:
        bool: 如果文件过期返回True，否则返回False
    """
    try:
        if not os.path.exists(file_path):
            return True
        
        # 获取文件最后修改时间
        last_modify_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        last_modify_time = last_modify_time.replace(tzinfo=timezone.utc).astimezone(Config.BEIJING_TIMEZONE)
        
        # 计算距离上次更新的天数
        days_since_update = (get_beijing_time() - last_modify_time).days
        need_update = days_since_update >= max_age_days
        
        if need_update:
            logger.info(f"文件 {file_path} 已过期({days_since_update}天)，需要更新")
        else:
            logger.debug(f"文件 {file_path} 未过期({days_since_update}天)，无需更新")
        
        return need_update
    
    except Exception as e:
        logger.error(f"检查文件更新状态失败: {str(e)}")
        # 出错时保守策略是要求更新
        return True

def is_trading_day(date: datetime) -> bool:
    """
    判断是否为交易日
    
    Args:
        date: 日期
    
    Returns:
        bool: 如果是交易日返回True，否则返回False
    """
    try:
        # 简化实现，实际应使用中国股市交易日历
        # 周六、周日不是交易日
        if date.weekday() >= 5:  # 5=周六，6=周日
            return False
        
        # TODO: 添加节假日判断
        
        return True
    
    except Exception as e:
        logger.error(f"判断交易日失败: {str(e)}", exc_info=True)
        # 出错时保守策略是认为是交易日
        return True

def calculate_atr(df: pd.DataFrame, period: int = 14) -> float:
    """
    计算平均真实波幅（ATR）
    
    Args:
        df: 价格数据DataFrame
        period: 计算周期
    
    Returns:
        float: ATR值
    """
    try:
        if len(df) < period:
            return 0.0
        
        # 确保列名正确
        high_col = "最高"
        low_col = "最低"
        close_col = "收盘"
        
        if high_col not in df.columns or low_col not in df.columns or close_col not in df.columns:
            logger.warning("DataFrame缺少必要列，无法计算ATR")
            return 0.0
        
        # 计算真实波幅
        df['prev_close'] = df[close_col].shift(1)
        df['tr1'] = df[high_col] - df[low_col]
        df['tr2'] = abs(df[high_col] - df['prev_close'])
        df['tr3'] = abs(df[low_col] - df['prev_close'])
        df['tr'] = df[['tr1', 'tr2', 'tr3']].max(axis=1)
        
        # 计算ATR
        df['atr'] = df['tr'].rolling(period).mean()
        
        # 返回最新ATR值
        return df['atr'].iloc[-1]
    
    except Exception as e:
        logger.error(f"计算ATR失败: {str(e)}", exc_info=True)
        return 0.0

def determine_market_condition() -> str:
    """
    确定当前市场环境
    
    Returns:
        str: 市场环境（'bull', 'bear', 'sideways'）
    """
    try:
        # 这里简化处理，实际应基于市场指数数据
        # 例如：使用沪深300指数判断市场环境
        
        # 假设我们有市场指数数据
        # market_data = load_market_index_data("000300.SH")  # 沪深300指数
        
        # 由于数据限制，这里使用简单逻辑
        # 实际应用中应基于真实市场数据
        
        # 获取最近30天的模拟市场数据
        dates = pd.date_range(end=datetime.now(), periods=30, freq='D')
        prices = np.random.normal(0.001, 0.02, 30).cumsum() + 1  # 模拟价格走势
        
        # 计算短期趋势（5日）
        short_ma = pd.Series(prices).rolling(5).mean().iloc[-1]
        current_price = prices[-1]
        short_trend = (current_price - short_ma) / short_ma
        
        # 计算中期趋势（20日）
        mid_ma = pd.Series(prices).rolling(20).mean().iloc[-1]
        mid_trend = (current_price - mid_ma) / mid_ma
        
        # 计算波动率
        volatility = pd.Series(prices).pct_change().std() * np.sqrt(252)
        
        # 计算动量
        momentum = (prices[-1] / prices[0]) - 1
        
        # 判断市场环境
        thresholds = Config.MARKET_CONDITION_THRESHOLDS
        if short_trend > thresholds['bull_short_trend'] and mid_trend > thresholds['bull_mid_trend'] and momentum > thresholds['bull_momentum']:
            return "bull"  # 牛市
        elif short_trend < thresholds['bear_short_trend'] and mid_trend < thresholds['bear_mid_trend'] and momentum < thresholds['bear_momentum']:
            return "bear"  # 熊市
        else:
            # 检查是否为震荡市（波动率高但趋势不明显）
            if volatility > thresholds['sideways_volatility'] and abs(short_trend) < thresholds['sideways_trend']:
                return "sideways"  # 震荡市
            else:
                # 默认跟随中期趋势
                return "bull" if mid_trend > 0 else "bear"
    
    except Exception as e:
        logger.error(f"确定市场环境失败: {str(e)}")
        return "sideways"  # 默认震荡市

# 模块初始化
try:
    # 验证时区设置
    utc_time = get_utc_time()
    beijing_time = get_beijing_time()
    
    logger.info(f"当前UTC时间: {utc_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"当前北京时间: {beijing_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 验证时区偏移
    time_diff = (beijing_time - utc_time).total_seconds() / 3600
    if abs(time_diff - 8) > 0.01:  # 允许0.01小时的误差
        logger.warning(f"时区偏移不正确: 北京时间比UTC时间快 {time_diff:.2f} 小时")
    else:
        logger.info("时区设置验证通过")
    
except Exception as e:
    logger.error(f"时区验证失败: {str(e)}", exc_info=True)