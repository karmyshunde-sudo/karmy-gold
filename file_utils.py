#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件操作工具模块
提供文件读写、标志文件管理、目录操作等常用功能
特别优化了增量数据保存功能
"""

import os
import logging
import pandas as pd
from datetime import datetime
from config import Config
from date_utils import get_beijing_time

# 初始化日志
logger = logging.getLogger(__name__)

def load_etf_daily_data(etf_code: str, data_dir: str = None) -> pd.DataFrame:
    """
    加载ETF日线数据
    
    Args:
        etf_code: ETF代码
        data_dir: 数据目录路径（可选）
    
    Returns:
        pd.DataFrame: ETF日线数据
    """
    try:
        if data_dir is None:
            data_dir = Config.ETFS_DAILY_DIR
        
        file_path = os.path.join(data_dir, f"{etf_code}.csv")
        if not os.path.exists(file_path):
            logger.debug(f"ETF {etf_code} 日线数据文件不存在: {file_path}")
            return pd.DataFrame()
        
        # 尝试读取CSV文件
        df = pd.read_csv(file_path, encoding="utf-8")
        
        # 确保日期列是datetime类型
        if "日期" in df.columns:
            df["日期"] = pd.to_datetime(df["日期"])
        
        return df
    
    except Exception as e:
        logger.error(f"加载ETF {etf_code} 日线数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def load_etf_metadata() -> pd.DataFrame:
    """
    加载ETF元数据
    
    Returns:
        pd.DataFrame: ETF元数据
    """
    try:
        if not os.path.exists(Config.METADATA_PATH):
            logger.warning("ETF元数据文件不存在")
            return pd.DataFrame()
        
        # 读取元数据
        metadata_df = pd.read_csv(Config.METADATA_PATH, encoding="utf-8-sig")
        
        return metadata_df
    
    except Exception as e:
        logger.error(f"加载ETF元数据失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def save_etf_score_history(score_df: pd.DataFrame):
    """
    保存ETF评分历史
    
    Args:
        score_df: 评分DataFrame
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(Config.SCORE_HISTORY_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if os.path.exists(Config.SCORE_HISTORY_PATH):
            # 读取现有记录
            history_df = pd.read_csv(Config.SCORE_HISTORY_PATH, encoding="utf-8")
        else:
            # 创建新的DataFrame
            history_df = pd.DataFrame(columns=["日期", "ETF代码", "ETF名称", "评分", "排名"])
        
        # 获取当前日期
        current_date = get_beijing_time().strftime("%Y-%m-%d")
        
        # 添加新记录
        new_records = []
        for i, row in score_df.iterrows():
            new_records.append({
                "日期": current_date,
                "ETF代码": row["etf_code"],
                "ETF名称": row["etf_name"],
                "评分": row["score"],
                "排名": i + 1
            })
        
        # 添加新记录到历史
        new_records_df = pd.DataFrame(new_records)
        history_df = pd.concat([history_df, new_records_df], ignore_index=True)
        
        # 保存记录
        history_df.to_csv(Config.SCORE_HISTORY_PATH, index=False, encoding="utf-8-sig")
        logger.info(f"ETF评分历史已保存，共{len(history_df)}条记录")
    
    except Exception as e:
        logger.error(f"保存ETF评分历史失败: {str(e)}", exc_info=True)

def init_dirs():
    """
    初始化必要目录
    """
    try:
        # 确保数据目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.ETFS_DAILY_DIR, exist_ok=True)
        os.makedirs(Config.FLAG_DIR, exist_ok=True)
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        
        logger.info("目录初始化完成")
    
    except Exception as e:
        logger.error(f"目录初始化失败: {str(e)}", exc_info=True)

def save_risk_monitor_record(risk_level: dict):
    """
    保存风险监控记录
    
    Args:
        risk_level: 风险水平字典
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(Config.RISK_MONITOR_FILE), exist_ok=True)
        
        # 准备记录数据
        beijing_now = get_beijing_time()
        record = {
            "时间": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
            "组合波动率": risk_level["portfolio_volatility"],
            "1日VaR": risk_level["var_1d"],
            "最大回撤预警": risk_level["max_drawdown_warning"],
            "流动性风险": risk_level["liquidity_risk"],
            "跟踪误差风险": risk_level["tracking_risk"],
            "相关性风险": risk_level["correlation_risk"],
            "综合风险水平": risk_level["overall_risk_level"],
            "风险提示": risk_level["risk_alert"]
        }
        
        # 检查文件是否存在
        if os.path.exists(Config.RISK_MONITOR_FILE):
            # 读取现有记录
            risk_df = pd.read_csv(Config.RISK_MONITOR_FILE, encoding="utf-8")
        else:
            # 创建新的DataFrame
            risk_df = pd.DataFrame(columns=list(record.keys()))
        
        # 添加新记录
        risk_df = pd.concat([risk_df, pd.DataFrame([record])], ignore_index=True)
        
        # 保存记录
        risk_df.to_csv(Config.RISK_MONITOR_FILE, index=False, encoding="utf-8-sig")
        logger.info("风险监控记录已保存")
    
    except Exception as e:
        logger.error(f"保存风险监控记录失败: {str(e)}", exc_info=True)

# 模块初始化
try:
    init_dirs()
    logger.info("文件工具模块初始化完成")
except Exception as e:
    logger.error(f"文件工具模块初始化失败: {str(e)}", exc_info=True)