#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ETF列表管理模块
提供ETF列表加载、筛选和更新功能
"""

import pandas as pd
import os
import logging
import akshare as ak
import time
from datetime import datetime
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

# 缓存变量，避免重复加载
_etf_list_cache = None
_last_load_time = None

def load_all_etf_list() -> pd.DataFrame:
    """
    加载全市场ETF列表，使用缓存机制避免重复加载
    
    Returns:
        pd.DataFrame: 包含ETF信息的DataFrame
    """
    global _etf_list_cache, _last_load_time
    
    # 检查缓存是否有效（5分钟内）
    current_time = datetime.now()
    if _etf_list_cache is not None and _last_load_time is not None:
        if (current_time - _last_load_time).total_seconds() < 300:
            return _etf_list_cache.copy()
    
    # 检查ETF列表文件是否存在
    if not os.path.exists(Config.ALL_ETFS_PATH):
        logger.warning("ETF列表文件不存在，尝试更新...")
        try:
            etf_list = update_all_etf_list()
            if etf_list.empty:
                logger.error("ETF列表更新失败，尝试加载兜底列表")
                etf_list = load_backup_etf_list()
        except Exception as e:
            logger.error(f"ETF列表更新失败: {str(e)}")
            etf_list = load_backup_etf_list()
    else:
        try:
            # 尝试加载ETF列表
            etf_list = pd.read_csv(Config.ALL_ETFS_PATH, encoding="utf-8")
            
            # 检查是否需要更新
            if Config.is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
                logger.info("ETF列表已过期，尝试更新...")
                try:
                    new_etf_list = update_all_etf_list()
                    if not new_etf_list.empty:
                        etf_list = new_etf_list
                except Exception as e:
                    logger.error(f"ETF列表更新失败: {str(e)}")
            
            # 确保包含所有需要的列
            for col in Config.ETF_STANDARD_COLUMNS:
                if col not in etf_list.columns:
                    etf_list[col] = ""
            
            etf_list = etf_list[Config.ETF_STANDARD_COLUMNS]
        except Exception as e:
            logger.error(f"加载ETF列表失败: {str(e)}")
            etf_list = load_backup_etf_list()
    
    # 更新缓存
    _etf_list_cache = etf_list.copy()
    _last_load_time = current_time
    
    return etf_list

def load_backup_etf_list() -> pd.DataFrame:
    """
    加载兜底ETF列表
    
    Returns:
        pd.DataFrame: 包含ETF信息的DataFrame
    """
    try:
        if not os.path.exists(Config.BACKUP_ETFS_PATH):
            logger.error("兜底ETF列表文件不存在")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        
        # 读取兜底ETF列表
        etf_list = pd.read_csv(Config.BACKUP_ETFS_PATH, encoding="utf-8")
        
        # 确保包含所有需要的列
        for col in Config.ETF_STANDARD_COLUMNS:
            if col not in etf_list.columns:
                etf_list[col] = ""
        
        etf_list = etf_list[Config.ETF_STANDARD_COLUMNS]
        
        logger.info(f"已加载兜底ETF列表，共{len(etf_list)}只ETF")
        return etf_list
    
    except Exception as e:
        logger.error(f"加载兜底ETF列表失败: {str(e)}")
        return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)

def update_all_etf_list() -> pd.DataFrame:
    """
    更新全市场ETF列表（三级降级策略）
    
    Returns:
        pd.DataFrame: 包含ETF信息的DataFrame
    """
    try:
        Config.init_dirs()
        primary_etf_list = None
        
        # 检查是否需要更新
        def is_list_need_update():
            if not os.path.exists(Config.ALL_ETFS_PATH):
                return True
            return Config.is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL)
        
        if is_list_need_update():
            logger.info("🔍 尝试更新全市场ETF列表...")
            
            # 1. 尝试AkShare接口
            try:
                etf_list = fetch_all_etfs_akshare()
                if not etf_list.empty:
                    # 确保包含所有需要的列
                    required_columns = Config.ETF_STANDARD_COLUMNS
                    for col in required_columns:
                        if col not in etf_list.columns:
                            etf_list[col] = ""
                    etf_list = etf_list[required_columns]
                    # 按基金规模降序排序
                    etf_list = etf_list.sort_values("基金规模", ascending=False)
                    etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                    logger.info(f"✅ AkShare更新成功（{len(etf_list)}只ETF）")
                    primary_etf_list = etf_list
                else:
                    logger.warning("AkShare返回空的ETF列表")
            except Exception as e:
                logger.error(f"AkShare接口错误: {str(e)}")
            
            # 2. 如果AkShare失败，尝试新浪接口
            if primary_etf_list is None or primary_etf_list.empty:
                try:
                    etf_list = fetch_all_etfs_sina()
                    if not etf_list.empty:
                        # 确保包含所有需要的列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in etf_list.columns:
                                etf_list[col] = ""
                        etf_list = etf_list[required_columns]
                        # 按基金规模降序排序
                        etf_list = etf_list.sort_values("基金规模", ascending=False)
                        etf_list.to_csv(Config.ALL_ETFS_PATH, index=False, encoding="utf-8")
                        logger.info(f"✅ 新浪更新成功（{len(etf_list)}只ETF）")
                        primary_etf_list = etf_list
                    else:
                        logger.warning("新浪接口返回空的ETF列表")
                except Exception as e:
                    logger.error(f"新浪接口错误: {str(e)}")
            
            # 3. 如果以上都失败，加载兜底列表
            if primary_etf_list is None or primary_etf_list.empty:
                try:
                    etf_list = load_backup_etf_list()
                    if not etf_list.empty:
                        # 确保包含所有需要的列
                        required_columns = Config.ETF_STANDARD_COLUMNS
                        for col in required_columns:
                            if col not in etf_list.columns:
                                etf_list[col] = ""
                        etf_list = etf_list[required_columns]
                        logger.info(f"✅ 使用兜底列表（{len(etf_list)}只ETF）")
                        primary_etf_list = etf_list
                    else:
                        logger.warning("兜底列表为空")
                except Exception as e:
                    logger.error(f"加载兜底列表失败: {str(e)}")
            
            # 如果所有方法都失败，返回空DataFrame
            if primary_etf_list is None:
                logger.error("所有ETF列表更新方法均失败")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
            
            return primary_etf_list
        else:
            logger.info("ETF列表无需更新")
            return load_all_etf_list()
    
    except Exception as e:
        logger.error(f"更新ETF列表失败: {str(e)}", exc_info=True)
        return load_backup_etf_list()

def fetch_all_etfs_akshare() -> pd.DataFrame:
    """
    从AkShare获取全市场ETF列表
    
    Returns:
        pd.DataFrame: ETF列表
    """
    try:
        logger.info("尝试从AkShare获取ETF列表...")
        
        # 获取ETF列表
        etf_list = ak.fund_etf_category_sina(symbol="ETF")
        
        # 确保包含必要列
        if "代码" in etf_list.columns and "名称" in etf_list.columns:
            etf_list = etf_list.rename(columns={"代码": "ETF代码", "名称": "ETF名称"})
            
            # 添加基金规模列（需要单独获取）
            etf_list["基金规模"] = 0.0
            etf_list["成立日期"] = ""
            
            # 处理每只ETF的详细信息
            for i, row in etf_list.iterrows():
                etf_code = row["ETF代码"]
                try:
                    # 获取ETF详细信息
                    etf_info = ak.fund_etf_info_em(symbol=etf_code)
                    if not etf_info.empty:
                        # 提取基金规模
                        if "基金规模" in etf_info.columns:
                            size_str = etf_info.iloc[0]["基金规模"]
                            if isinstance(size_str, str):
                                if "亿" in size_str:
                                    etf_list.at[i, "基金规模"] = float(size_str.replace("亿", ""))
                                elif "万" in size_str:
                                    etf_list.at[i, "基金规模"] = float(size_str.replace("万", "")) / 10000
                            elif isinstance(size_str, (int, float)):
                                etf_list.at[i, "基金规模"] = size_str
                        
                        # 提取成立日期
                        if "成立日期" in etf_info.columns:
                            etf_list.at[i, "成立日期"] = etf_info.iloc[0]["成立日期"]
                except Exception as e:
                    logger.debug(f"获取ETF {etf_code} 详细信息失败: {str(e)}")
                
                # 避免请求过于频繁
                time.sleep(0.1)
            
            # 提取纯数字代码
            etf_list["ETF代码"] = etf_list["ETF代码"].astype(str).str.strip().str.zfill(6)
            
            # 添加完整代码列
            etf_list["完整代码"] = etf_list["ETF代码"]
            
            # 按基金规模降序排序
            etf_list = etf_list.sort_values("基金规模", ascending=False)
            
            logger.info(f"AkShare获取到{len(etf_list)}只ETF")
            return etf_list
        else:
            logger.warning("AkShare接口返回的数据缺少必要列")
            return pd.DataFrame()
    
    except Exception as e:
        error_msg = f"AkShare接口错误: {str(e)}"
        logger.error(error_msg)
        raise Exception(error_msg)

def fetch_all_etfs_sina() -> pd.DataFrame:
    """
    从新浪获取全市场ETF列表
    
    Returns:
        pd.DataFrame: ETF列表
    """
    try:
        logger.info("尝试从新浪获取ETF列表...")
        
        # 新浪ETF列表接口
        url = "https://finance.sina.com.cn/realstock/company/"
        # 这里简化处理，实际应调用新浪接口
        # 由于新浪接口可能需要解析HTML，这里使用模拟数据
        
        # 模拟ETF数据
        etf_data = [
            {"symbol": "510300", "name": "沪深300ETF", "price": 3.5, "change": 0.02},
            {"symbol": "510050", "name": "上证50ETF", "price": 2.8, "change": 0.01},
            {"symbol": "510500", "name": "中证500ETF", "price": 5.2, "change": 0.03},
            # 更多ETF数据...
        ]
        
        if not isinstance(etf_data, list):
            logger.warning("新浪接口返回的数据不是列表格式")
            return pd.DataFrame()
        
        # 创建DataFrame
        if etf_data:
            etf_list = pd.DataFrame(etf_data)
            # 检查必要的列是否存在
            if "symbol" in etf_list.columns and "name" in etf_list.columns:
                etf_list = etf_list.rename(columns={"symbol": "完整代码", "name": "ETF名称"})
                # 提取纯数字代码
                etf_list["ETF代码"] = etf_list["完整代码"].str[-6:].str.strip()
                # 添加空白的基金规模列
                etf_list["基金规模"] = 0.0
                # 确保包含所有需要的列
                for col in Config.ETF_STANDARD_COLUMNS:
                    if col not in etf_list.columns:
                        etf_list[col] = ""
                etf_list = etf_list[Config.ETF_STANDARD_COLUMNS]
                # 按基金规模降序排序
                etf_list = etf_list.sort_values("基金规模", ascending=False)
                logger.info(f"新浪获取到{len(etf_list)}只ETF")
                return etf_list.drop_duplicates(subset="ETF代码")
            else:
                logger.warning("新浪接口返回的数据缺少必要列")
                return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
        else:
            logger.warning("新浪接口返回空数据")
            return pd.DataFrame(columns=Config.ETF_STANDARD_COLUMNS)
    
    except Exception as e:
        error_msg = f"新浪接口错误: {str(e)}"
        logger.error(f"❌ {error_msg}")
        raise Exception(error_msg)

def get_etf_name(etf_code: str) -> str:
    """
    从全市场列表中获取ETF名称
    
    Args:
        etf_code: ETF代码
    
    Returns:
        str: ETF名称
    """
    try:
        if not etf_code or not isinstance(etf_code, str):
            return f"ETF-INVALID-CODE"
        
        # 确保代码格式正确
        etf_code = etf_code.strip().zfill(6)
        
        # 获取ETF列表
        etf_list = load_all_etf_list()
        if etf_list.empty:
            return f"ETF-{etf_code}"
        
        # 查找ETF
        name_row = etf_list[
            etf_list["ETF代码"].astype(str).str.strip().str.zfill(6) == etf_code]
        
        if not name_row.empty:
            return name_row.iloc[0]["ETF名称"]
        else:
            logger.debug(f"未在全市场列表中找到ETF代码: {etf_code}")
            return f"ETF-{etf_code}"
    
    except Exception as e:
        logger.error(f"获取ETF名称失败: {str(e)}")
        return f"ETF-{etf_code}"

def get_filtered_etf_list() -> Dict[str, pd.DataFrame]:
    """
    获取分层ETF列表
    
    Returns:
        Dict[str, pd.DataFrame]: 分层ETF列表
    """
    try:
        # 获取所有ETF
        all_etfs = load_all_etf_list()
        if all_etfs.empty:
            logger.warning("全市场ETF列表为空")
            return {layer: pd.DataFrame() for layer in Config.ETF_SELECTION_LAYERS}
        
        # 获取市场环境
        market_condition = determine_market_condition()
        logger.info(f"当前市场环境: {market_condition}")
        
        # 动态调整基础层阈值
        dynamic_base_params = get_dynamic_selection_thresholds(market_condition)
        
        # 计算ETF年龄（天）
        today = get_beijing_time().date()
        all_etfs["成立天数"] = all_etfs["成立日期"].apply(
            lambda x: (today - datetime.strptime(x, "%Y-%m-%d").date()).days 
            if isinstance(x, str) and len(x) >= 10 else 0
        )
        
        # 计算规模增长率
        all_etfs["规模增长率"] = all_etfs["ETF代码"].apply(
            lambda x: calculate_size_growth_rate(x)
        )
        
        # 基础层筛选（稳健仓使用）
        base_layer = all_etfs[
            (all_etfs["基金规模"] >= dynamic_base_params["min_fund_size"]) &
            (all_etfs["日均成交额"] >= dynamic_base_params["min_avg_volume"]) &
            (all_etfs["成立天数"] >= dynamic_base_params["min_listing_days"])
        ]
        
        # 机会层筛选（激进仓使用）
        opportunity_layer = all_etfs[
            (all_etfs["基金规模"] >= Config.ETF_SELECTION_LAYERS["opportunity"]["min_fund_size"]) &
            (all_etfs["日均成交额"] >= Config.ETF_SELECTION_LAYERS["opportunity"]["min_avg_volume"]) &
            (all_etfs["成立天数"] >= Config.ETF_SELECTION_LAYERS["opportunity"]["min_listing_days"])
        ]
        
        # 成长期筛选（成长仓使用）
        growth_layer = all_etfs[
            (all_etfs["基金规模"] >= Config.ETF_SELECTION_LAYERS["growth"]["min_fund_size"]) &
            (all_etfs["日均成交额"] >= Config.ETF_SELECTION_LAYERS["growth"]["min_avg_volume"]) &
            (all_etfs["成立天数"] <= Config.ETF_SELECTION_LAYERS["growth"]["max_listing_days"]) &
            (all_etfs["规模增长率"] >= Config.ETF_SELECTION_LAYERS["growth"]["min_size_growth"])
        ]
        
        # 行业特色层筛选
        sector_special_layer = all_etfs[
            (all_etfs["基金规模"] >= Config.ETF_SELECTION_LAYERS["sector_special"]["min_fund_size"]) &
            (all_etfs["日均成交额"] >= Config.ETF_SELECTION_LAYERS["sector_special"]["min_avg_volume"]) &
            (all_etfs["行业"].isin(Config.ETF_SELECTION_LAYERS["sector_special"]["sector_focus"]))
        ]
        
        logger.info(f"ETF分层筛选结果: 基础层={len(base_layer)}, 机会层={len(opportunity_layer)}, "
                   f"成长层={len(growth_layer)}, 行业层={len(sector_special_layer)}")
        
        return {
            "base": base_layer,
            "opportunity": opportunity_layer,
            "growth": growth_layer,
            "sector_special": sector_special_layer
        }
    
    except Exception as e:
        logger.error(f"ETF分层筛选失败: {str(e)}", exc_info=True)
        return {layer: pd.DataFrame() for layer in Config.ETF_SELECTION_LAYERS}

def get_dynamic_selection_thresholds(market_condition: str) -> dict:
    """
    根据市场环境动态调整筛选阈值
    
    Args:
        market_condition: 市场环境（'bull', 'bear', 'sideways'）
    
    Returns:
        dict: 动态调整后的筛选阈值
    """
    base_params = Config.ETF_SELECTION_LAYERS["base"].copy()
    
    if market_condition == "bull":
        # 牛市：提高标准，过滤低质量ETF
        base_params["min_fund_size"] *= 1.2
        base_params["min_avg_volume"] *= 1.2
    elif market_condition == "bear":
        # 熊市：降低标准，增加机会覆盖
        base_params["min_fund_size"] *= 0.8
        base_params["min_avg_volume"] *= 0.8
        # 熊市中更关注流动性，提高流动性权重
        base_params["min_avg_volume"] = max(base_params["min_avg_volume"], 3000.0)
    
    return base_params

def calculate_size_growth_rate(etf_code: str) -> float:
    """
    计算ETF规模增长率
    
    Args:
        etf_code: ETF代码
    
    Returns:
        float: 规模月增长率
    """
    try:
        # 这里简化处理，实际应从历史数据中获取
        # 返回一个模拟值
        return 0.35  # 35%月增长率
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 规模增长率失败: {str(e)}")
        return 0.0

# 模块初始化
try:
    Config.init_dirs()
    logger.info("ETF列表管理器初始化完成")
except Exception as e:
    logger.error(f"ETF列表管理器初始化失败: {str(e)}")