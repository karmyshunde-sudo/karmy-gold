#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
仓位策略计算模块
负责计算稳健仓、激进仓、成长仓和行业仓的操作策略
特别优化了消息推送格式，支持多条详细消息推送
"""

import pandas as pd
import os
import numpy as np
import logging
import sys
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from config import Config
from date_utils import (
    get_current_times,
    get_beijing_time,
    get_utc_time,
    is_file_outdated,
    calculate_atr,
    determine_market_condition
)
from file_utils import (
    load_etf_daily_data, 
    init_dirs, 
    save_risk_monitor_record,
    load_etf_metadata
)
from etf_scoring import (
    get_top_rated_etfs,
    get_etf_name,
    get_etf_basic_info,
    calculate_tracking_error,
    calculate_liquidity_score
)
from wechat_push import send_multiple_messages
from etf_list import load_all_etf_list, get_filtered_etf_list

# 初始化日志
logger = logging.getLogger(__name__)

# 仓位持仓记录路径
POSITION_RECORD_PATH = Config.POSITION_RECORD_FILE
TRADE_RECORD_PATH = Config.TRADE_RECORD_FILE

def init_position_record() -> pd.DataFrame:
    """
    初始化仓位记录（稳健仓、激进仓各持多只ETF）
    
    Returns:
        pd.DataFrame: 仓位记录的DataFrame
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(POSITION_RECORD_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if os.path.exists(POSITION_RECORD_PATH):
            # 读取现有记录
            position_df = pd.read_csv(POSITION_RECORD_PATH, encoding="utf-8-sig")
            
            # 确保包含所有必要列
            required_columns = ["仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量", "权重"]
            for col in required_columns:
                if col not in position_df.columns:
                    position_df[col] = ""
            
            return position_df[required_columns]
        else:
            # 创建默认仓位记录
            default_positions = [
                {
                    "仓位类型": "稳健仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "权重": 0.0
                },
                {
                    "仓位类型": "激进仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "权重": 0.0
                },
                {
                    "仓位类型": "套利仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "权重": 0.0
                },
                {
                    "仓位类型": "成长仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "权重": 0.0
                },
                {
                    "仓位类型": "行业仓",
                    "ETF代码": "",
                    "ETF名称": "",
                    "持仓成本价": 0.0,
                    "持仓日期": "",
                    "持仓数量": 0,
                    "权重": 0.0
                }
            ]
            return pd.DataFrame(default_positions)
    
    except Exception as e:
        error_msg = f"初始化仓位记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回空DataFrame但包含必要列
        return pd.DataFrame(columns=["仓位类型", "ETF代码", "ETF名称", "持仓成本价", "持仓日期", "持仓数量", "权重"])

def calculate_position_strategy() -> List[str]:
    """
    计算仓位策略并生成多条详细操作建议
    
    Returns:
        List[str]: 多条格式化后的消息（纯业务内容，不含分页标识）
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        logger.info(f"开始计算仓位策略 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 获取当前持仓
        current_positions = init_position_record()
        
        # 获取市场环境
        market_condition = determine_market_condition()
        logger.info(f"当前市场环境: {market_condition}")
        
        # 获取组合风险水平
        risk_level = monitor_portfolio_risk(current_positions)
        logger.info(f"当前组合风险水平: {risk_level['overall_risk_level']}")
        
        # 生成策略建议
        strategy_suggestions = {}
        trade_actions = []
        
        # 1. 基础仓位策略
        for position_type in ["稳健仓", "激进仓", "成长仓", "行业仓"]:
            # 获取当前仓位持仓
            current_position = current_positions[current_positions["仓位类型"] == position_type].iloc[0]
            
            # 获取高评分ETF列表
            top_etfs = get_top_rated_etfs(position_type=position_type)
            
            # 生成操作建议
            suggestion, actions = generate_position_suggestion(
                position_type, 
                top_etfs, 
                current_position,
                market_condition,
                risk_level
            )
            
            strategy_suggestions[position_type] = suggestion
            trade_actions.extend(actions)
        
        # 2. 机会池评估（额外机会发现）
        opportunity_suggestion = evaluate_opportunity_pool(market_condition, risk_level)
        strategy_suggestions["机会发现"] = opportunity_suggestion
        
        # 生成多条详细消息（纯业务内容，不含分页标识）
        messages = generate_detailed_messages(strategy_suggestions, risk_level)
        
        # 记录交易动作
        if trade_actions:
            record_trade_actions(trade_actions)
        
        # 记录风险监控
        save_risk_monitor_record(risk_level)
        
        logger.info(f"仓位策略计算完成，生成 {len(messages)} 条详细消息")
        return messages
    
    except Exception as e:
        error_msg = f"仓位策略计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回错误消息
        return [f"仓位策略计算失败，请检查系统日志"]

def generate_detailed_messages(strategy_suggestions: Dict[str, str], risk_level: dict) -> List[str]:
    """
    生成多条详细消息（只包含纯业务内容，无格式）
    
    Args:
        strategy_suggestions: 策略建议字典
        risk_level: 风险水平
    
    Returns:
        List[str]: 多条详细消息（纯业务内容）
    """
    try:
        messages = []
        total_messages = 5  # 总消息数
        
        # 1. 稳健仓消息
        if "稳健仓" in strategy_suggestions:
            message = "【Karmy-Gold ETF仓位操作提示】\n"
            message += "（组合化投资，每仓位持有2-4只低相关性ETF）\n\n"
            message += "【稳健仓】\n"
            message += strategy_suggestions["稳健仓"]
            
            # 添加稳健仓持仓详情
            current_positions = init_position_record()
            stable_position = current_positions[current_positions["仓位类型"] == "稳健仓"].iloc[0]
            
            if not pd.isna(stable_position["ETF代码"]) and stable_position["ETF代码"]:
                etf_codes = [code.strip() for code in str(stable_position["ETF代码"]).split(",") if code.strip()]
                for code in etf_codes:
                    message += format_etf_holding_detail(code, "稳健仓")
            
            # 添加风险提示
            message += f"\n• 风险提示: 当前风险水平 {risk_level['overall_risk_level'].upper()}\n"
            if risk_level["max_drawdown_warning"] > Config.STRATEGY_PARAMETERS["稳健仓"]["max_drawdown_warning"]:
                message += f"  - 警告: 组合最大回撤 {risk_level['max_drawdown_warning']:.1%} 超过阈值 {Config.STRATEGY_PARAMETERS['稳健仓']['max_drawdown_warning']:.1%}\n"
            
            messages.append(message)
        
        # 2. 激进仓消息
        if "激进仓" in strategy_suggestions:
            message = "【Karmy-Gold ETF仓位操作提示】\n"
            message += "（组合化投资，每仓位持有2-4只低相关性ETF）\n\n"
            message += "【激进仓】\n"
            message += strategy_suggestions["激进仓"]
            
            # 添加激进仓持仓详情
            current_positions = init_position_record()
            aggressive_position = current_positions[current_positions["仓位类型"] == "激进仓"].iloc[0]
            
            if not pd.isna(aggressive_position["ETF代码"]) and aggressive_position["ETF代码"]:
                etf_codes = [code.strip() for code in str(aggressive_position["ETF代码"]).split(",") if code.strip()]
                for code in etf_codes:
                    message += format_etf_holding_detail(code, "激进仓")
            
            # 添加风险提示
            message += f"\n• 风险提示: 当前风险水平 {risk_level['overall_risk_level'].upper()}\n"
            if risk_level["max_drawdown_warning"] > Config.STRATEGY_PARAMETERS["激进仓"]["max_drawdown_warning"]:
                message += f"  - 警告: 组合最大回撤 {risk_level['max_drawdown_warning']:.1%} 超过阈值 {Config.STRATEGY_PARAMETERS['激进仓']['max_drawdown_warning']:.1%}\n"
            
            messages.append(message)
        
        # 3. 成长仓消息
        if "成长仓" in strategy_suggestions:
            message = "【Karmy-Gold ETF仓位操作提示】\n"
            message += "（组合化投资，每仓位持有2-4只低相关性ETF）\n\n"
            message += "【成长仓】\n"
            message += strategy_suggestions["成长仓"]
            
            # 添加成长仓持仓详情
            current_positions = init_position_record()
            growth_position = current_positions[current_positions["仓位类型"] == "成长仓"].iloc[0]
            
            if not pd.isna(growth_position["ETF代码"]) and growth_position["ETF代码"]:
                etf_codes = [code.strip() for code in str(growth_position["ETF代码"]).split(",") if code.strip()]
                for code in etf_codes:
                    message += format_etf_holding_detail(code, "成长仓")
            
            # 添加风险提示
            message += f"\n• 风险提示: 当前风险水平 {risk_level['overall_risk_level'].upper()}\n"
            if risk_level["max_drawdown_warning"] > Config.STRATEGY_PARAMETERS["成长仓"]["max_drawdown_warning"]:
                message += f"  - 警告: 组合最大回撤 {risk_level['max_drawdown_warning']:.1%} 超过阈值 {Config.STRATEGY_PARAMETERS['成长仓']['max_drawdown_warning']:.1%}\n"
            
            messages.append(message)
        
        # 4. 行业仓消息
        if "行业仓" in strategy_suggestions:
            message = "【Karmy-Gold ETF仓位操作提示】\n"
            message += "（组合化投资，每仓位持有2-4只低相关性ETF）\n\n"
            message += "【行业仓】\n"
            message += strategy_suggestions["行业仓"]
            
            # 添加行业仓持仓详情
            current_positions = init_position_record()
            sector_position = current_positions[current_positions["仓位类型"] == "行业仓"].iloc[0]
            
            if not pd.isna(sector_position["ETF代码"]) and sector_position["ETF代码"]:
                etf_codes = [code.strip() for code in str(sector_position["ETF代码"]).split(",") if code.strip()]
                for code in etf_codes:
                    message += format_etf_holding_detail(code, "行业仓")
            
            # 添加风险提示
            message += f"\n• 风险提示: 当前风险水平 {risk_level['overall_risk_level'].upper()}\n"
            if risk_level["max_drawdown_warning"] > Config.STRATEGY_PARAMETERS["行业仓"]["max_drawdown_warning"]:
                message += f"  - 警告: 组合最大回撤 {risk_level['max_drawdown_warning']:.1%} 超过阈值 {Config.STRATEGY_PARAMETERS['行业仓']['max_drawdown_warning']:.1%}\n"
            
            messages.append(message)
        
        # 5. 机会发现和风险汇总消息
        if "机会发现" in strategy_suggestions:
            message = "【Karmy-Gold ETF仓位操作提示】\n"
            message += "（组合化投资，每仓位持有2-4只低相关性ETF）\n\n"
            message += "【机会发现与风险汇总】\n"
            message += strategy_suggestions["机会发现"]
            
            # 添加风险汇总
            message += "\n【全仓风险汇总】\n"
            message += f"• 组合波动率: {risk_level['portfolio_volatility']:.4f}\n"
            message += f"• 1日VaR: {risk_level['var_1d']:.4f}\n"
            message += f"• 最大回撤预警: {risk_level['max_drawdown_warning']:.4f}\n"
            message += f"• 流动性风险: {risk_level['liquidity_risk']:.4f}\n"
            message += f"• 跟踪误差风险: {risk_level['tracking_risk']:.4f}\n"
            message += f"• 相关性风险: {risk_level['correlation_risk']:.4f}\n"
            message += f"• 综合风险水平: {risk_level['overall_risk_level'].upper()}\n"
            message += f"• 风险提示: {risk_level['risk_alert']}\n"
            
            # 添加投资建议
            message += "\n【投资建议】\n"
            if risk_level['overall_risk_level'] == "low":
                message += "• 市场风险较低，可维持当前仓位策略，适当增加持仓比例\n"
            elif risk_level['overall_risk_level'] == "medium":
                message += "• 市场风险中等，建议保持当前仓位，关注市场动向\n"
            else:
                message += "• 市场风险较高，建议降低仓位比例，增加防御性资产\n"
            
            messages.append(message)
        
        return messages
    
    except Exception as e:
        error_msg = f"生成详细消息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回单条错误消息
        return [f"生成详细消息失败: {str(e)}"]

def format_etf_holding_detail(etf_code: str, position_type: str) -> str:
    """
    格式化ETF持仓详情
    
    Args:
        etf_code: ETF代码
        position_type: 仓位类型
    
    Returns:
        str: 格式化后的持仓详情
    """
    try:
        # 获取ETF名称
        etf_name = get_etf_name(etf_code)
        
        # 获取持仓信息
        current_positions = init_position_record()
        position = current_positions[current_positions["仓位类型"] == position_type].iloc[0]
        
        # 获取持仓成本和日期
        cost_price = 0.0
        hold_date = ""
        if not pd.isna(position["ETF代码"]) and position["ETF代码"]:
            etf_codes = [code.strip() for code in str(position["ETF代码"]).split(",") if code.strip()]
            etf_costs = [float(price.strip()) for price in str(position["持仓成本价"]).split(",") if price.strip()]
            etf_dates = [date.strip() for date in str(position["持仓日期"]).split(",") if date.strip()]
            
            if etf_code in etf_codes:
                idx = etf_codes.index(etf_code)
                if idx < len(etf_costs):
                    cost_price = etf_costs[idx]
                if idx < len(etf_dates):
                    hold_date = etf_dates[idx]
        
        # 获取最新价格
        df = load_etf_daily_data(etf_code)
        latest_price = df.iloc[-1]["收盘"] if not df.empty else cost_price
        
        # 计算收益率
        return_rate = 0.0
        if cost_price > 0:
            return_rate = (latest_price - cost_price) / cost_price
        
        # 计算支撑位和压力位
        support, resistance = calculate_support_resistance(etf_code)
        
        # 格式化持仓详情
        detail = f"\n• 持有{etf_code}【{hold_date}买入价{cost_price:.2f}元"
        if support > 0 and resistance > 0:
            detail += f"，支撑位：{support:.2f}元，压力位：{resistance:.2f}元"
        detail += f"，收益率：{return_rate:.2%}】\n"
        
        return detail
    
    except Exception as e:
        logger.error(f"格式化ETF {etf_code} 持仓详情失败: {str(e)}", exc_info=True)
        return f"\n• 持有{etf_code}【持仓详情获取失败】\n"

def calculate_support_resistance(etf_code: str) -> Tuple[float, float]:
    """
    计算支撑位和压力位
    
    Args:
        etf_code: ETF代码
    
    Returns:
        Tuple[float, float]: (支撑位, 压力位)
    """
    try:
        # 获取ETF日线数据
        df = load_etf_daily_data(etf_code)
        if df.empty:
            return 0.0, 0.0
        
        # 计算20日最高价和最低价
        recent_20d = df.tail(20)
        if recent_20d.empty:
            return 0.0, 0.0
        
        high_20d = recent_20d["最高"].max()
        low_20d = recent_20d["最低"].min()
        
        # 计算中间价
        mid_price = (high_20d + low_20d) / 2
        
        # 计算支撑位和压力位
        support = mid_price - (high_20d - low_20d) * 0.382
        resistance = mid_price + (high_20d - low_20d) * 0.382
        
        return support, resistance
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 支撑位和压力位失败: {str(e)}", exc_info=True)
        return 0.0, 0.0

def generate_position_suggestion(
    position_type: str, 
    top_etfs: pd.DataFrame, 
    current_position: pd.Series,
    market_condition: str,
    risk_level: dict
) -> Tuple[str, List[Dict]]:
    """
    生成仓位操作建议
    
    Args:
        position_type: 仓位类型
        top_etfs: 高评分ETF列表
        current_position: 当前持仓
        market_condition: 市场环境
        risk_level: 风险水平
    
    Returns:
        Tuple[str, List[Dict]]: (策略描述, 交易动作列表)
    """
    try:
        if top_etfs.empty or len(top_etfs) < 2:
            return f"• 无足够ETF数据，无法生成{position_type}策略建议", []
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        current_date = beijing_now.strftime("%Y-%m-%d")
        
        trade_actions = []
        params = Config.STRATEGY_PARAMETERS[position_type]
        
        # 计算目标仓位比例
        target_position_size = adjust_position_size(
            risk_level["overall_risk_level"],
            market_condition,
            risk_level["portfolio_volatility"]
        )
        
        # 获取当前持仓ETF列表
        current_etfs = []
        if not pd.isna(current_position["ETF代码"]) and current_position["ETF代码"]:
            current_etfs = [code.strip() for code in str(current_position["ETF代码"]).split(",") if code.strip()]
        
        # 获取最优ETF组合
        optimal_portfolio = get_optimal_portfolio(position_type, top_etfs)
        
        if optimal_portfolio.empty:
            return f"• 未找到合适的ETF组合，保持当前持仓", []
        
        # 1. 检查是否需要换仓
        need_switch = False
        switch_reason = ""
        
        if current_etfs:
            # 检查当前持仓ETF是否在最优组合中
            current_in_optimal = all(code in optimal_portfolio["etf_code"].values for code in current_etfs)
            
            if not current_in_optimal:
                need_switch = True
                switch_reason = "当前持仓ETF不在最优组合中"
            
            # 检查评分差距
            if not need_switch:
                for code in current_etfs:
                    current_score = get_etf_score(code)
                    if current_score < 60:
                        need_switch = True
                        switch_reason = f"持仓ETF {code} 评分过低({current_score})"
                        break
        
        # 2. 检查是否需要调整仓位大小
        position_change_needed = False
        current_size = len(current_etfs) if current_etfs else 0
        target_size = len(optimal_portfolio)
        
        # 3. 生成操作建议
        suggestion = []
        
        if need_switch:
            # 计算目标组合中不在当前持仓的ETF
            new_etfs = [row for _, row in optimal_portfolio.iterrows() 
                        if row["etf_code"] not in current_etfs]
            
            # 计算当前持仓中不在目标组合的ETF
            old_etfs = [code for code in current_etfs if code not in optimal_portfolio["etf_code"].values]
            
            # 生成换仓建议
            if old_etfs and new_etfs:
                # 先卖后买
                for code in old_etfs:
                    trade_actions.append({
                        "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "持仓类型": position_type,
                        "ETF代码": code,
                        "ETF名称": get_etf_name(code),
                        "价格": load_etf_daily_data(code).iloc[-1]["收盘"],
                        "数量": "全部",
                        "操作": "卖出",
                        "备注": f"换仓: {switch_reason}"
                    })
                
                for _, row in optimal_portfolio.iterrows():
                    trade_actions.append({
                        "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "持仓类型": position_type,
                        "ETF代码": row["etf_code"],
                        "ETF名称": row["etf_name"],
                        "价格": load_etf_daily_data(row["etf_code"]).iloc[-1]["收盘"],
                        "数量": "按权重",
                        "操作": "买入",
                        "备注": f"换入最优组合"
                    })
                
                suggestion.append(f"• 需要换仓: {switch_reason}")
                suggestion.append(f"  - 卖出: {', '.join(old_etfs)}")
                suggestion.append(f"  - 买入: {', '.join([row['etf_code'] for _, row in optimal_portfolio.iterrows()])}")
            elif new_etfs:  # 只有新增，没有移除
                for _, row in optimal_portfolio.iterrows():
                    if row["etf_code"] not in current_etfs:
                        trade_actions.append({
                            "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                            "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                            "持仓类型": position_type,
                            "ETF代码": row["etf_code"],
                            "ETF名称": row["etf_name"],
                            "价格": load_etf_daily_data(row["etf_code"]).iloc[-1]["收盘"],
                            "数量": "按权重",
                            "操作": "买入",
                            "备注": "新增持仓"
                        })
                
                suggestion.append(f"• 需要新增持仓: {switch_reason}")
                suggestion.append(f"  - 买入: {', '.join([row['etf_code'] for _, row in optimal_portfolio.iterrows() if row['etf_code'] not in current_etfs])}")
        
        # 4. 检查止损条件
        stop_loss_actions = []
        for code in current_etfs:
            df = load_etf_daily_data(code)
            if df.empty:
                continue
            
            # 获取动态止损比例
            stop_loss_ratio = calculate_dynamic_stop_loss(code, df, position_type)
            
            # 检查是否触发止损
            current_price = df.iloc[-1]["收盘"]
            cost_price = current_position["持仓成本价"]
            
            if cost_price > 0:
                loss_ratio = (current_price - cost_price) / cost_price
                
                if loss_ratio <= -stop_loss_ratio:
                    stop_loss_actions.append({
                        "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "持仓类型": position_type,
                        "ETF代码": code,
                        "ETF名称": get_etf_name(code),
                        "价格": current_price,
                        "数量": "全部",
                        "操作": "止损卖出",
                        "备注": f"触发止损({stop_loss_ratio:.1%})"
                    })
                    suggestion.append(f"• 触发止损: ETF {code} 亏损 {loss_ratio:.1%} (止损线: {stop_loss_ratio:.1%})")
        
        if stop_loss_actions:
            trade_actions.extend(stop_loss_actions)
        
        # 5. 检查仓位大小调整
        current_position_size = len(current_etfs) if current_etfs else 0
        if abs(current_position_size - target_size) > 0.05:  # 差异超过5%
            # 调整仓位大小
            position_change = target_position_size - current_position_size
            
            if position_change > 0:  # 需要加仓
                # 选择最优组合中未持仓的ETF
                new_etfs = [row for _, row in optimal_portfolio.iterrows() 
                           if row["etf_code"] not in current_etfs]
                
                if new_etfs:
                    for row in new_etfs[:abs(int(position_change * target_size))]:
                        trade_actions.append({
                            "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                            "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                            "持仓类型": position_type,
                            "ETF代码": row["etf_code"],
                            "ETF名称": row["etf_name"],
                            "价格": load_etf_daily_data(row["etf_code"]).iloc[-1]["收盘"],
                            "数量": "按权重",
                            "操作": "加仓",
                            "备注": f"调整仓位至{target_position_size:.0%}"
                        })
                    suggestion.append(f"• 需要加仓: 仓位目标 {target_position_size:.0%}，当前 {current_position_size:.0%}")
            else:  # 需要减仓
                # 选择表现最差的ETF
                etfs_to_sell = []
                for code in current_etfs:
                    score = get_etf_score(code)
                    etfs_to_sell.append((code, score))
                
                etfs_to_sell.sort(key=lambda x: x[1])  # 按评分排序
                
                for code, _ in etfs_to_sell[:abs(int(position_change * current_position_size))]:
                    trade_actions.append({
                        "时间(UTC)": utc_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "时间(北京时间)": beijing_now.strftime("%Y-%m-%d %H:%M:%S"),
                        "持仓类型": position_type,
                        "ETF代码": code,
                        "ETF名称": get_etf_name(code),
                        "价格": load_etf_daily_data(code).iloc[-1]["收盘"],
                        "数量": "部分",
                        "操作": "减仓",
                        "备注": f"调整仓位至{target_position_size:.0%}"
                    })
                suggestion.append(f"• 需要减仓: 仓位目标 {target_position_size:.0%}，当前 {current_position_size:.0%}")
        
        # 6. 生成最终建议
        if not suggestion:
            suggestion.append("• 无需操作: 当前持仓符合最优组合要求")
        
        # 添加风险提示
        suggestion.append(f"• 风险提示: 当前风险水平 {risk_level['overall_risk_level'].upper()}")
        if risk_level["max_drawdown_warning"] > params["max_drawdown_warning"]:
            suggestion.append(f"  - 警告: 组合最大回撤 {risk_level['max_drawdown_warning']:.1%} 超过阈值 {params['max_drawdown_warning']:.1%}")
        
        return "\n".join(suggestion), trade_actions
    
    except Exception as e:
        error_msg = f"生成{position_type}仓位建议失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        return f"• {position_type}：生成建议时发生错误", []

def get_optimal_portfolio(position_type: str, top_etfs: pd.DataFrame) -> pd.DataFrame:
    """
    获取最优ETF组合（考虑相关性和风险）
    
    Args:
        position_type: 仓位类型（'稳健仓', '激进仓', '套利仓', '成长仓', '行业仓'）
        top_etfs: 高评分ETF列表
    
    Returns:
        pd.DataFrame: 最优ETF组合（包含代码、名称、权重等）
    """
    if top_etfs.empty:
        return pd.DataFrame()
    
    try:
        # 计算ETF之间的相关性
        correlation_matrix = calculate_correlation_matrix(top_etfs)
        
        # 根据仓位类型确定参数
        params = Config.STRATEGY_PARAMETERS.get(position_type, Config.STRATEGY_PARAMETERS["稳健仓"])
        max_holdings = params["max_holdings"]
        min_correlation = params["min_correlation"]
        
        # 选择低相关性的ETF
        selected_etfs = select_low_correlation_etfs(top_etfs, correlation_matrix, max_holdings, min_correlation)
        
        # 计算风险平价权重
        weights = calculate_risk_parity_weights(selected_etfs)
        
        # 创建组合DataFrame
        portfolio = selected_etfs.copy()
        portfolio["weight"] = weights
        
        return portfolio
    
    except Exception as e:
        logger.error(f"获取{position_type}最优组合失败: {str(e)}")
        return top_etfs.head(1)  # 返回评分最高的ETF

def calculate_correlation_matrix(etfs: pd.DataFrame) -> pd.DataFrame:
    """
    计算ETF之间的相关性矩阵
    
    Args:
        etfs: ETF列表
    
    Returns:
        pd.DataFrame: 相关性矩阵
    """
    try:
        # 这里简化处理，实际应从历史数据中计算相关性
        # 例如：计算ETF过去30天收益率的相关性
        
        etf_codes = etfs["etf_code"].tolist()
        n = len(etf_codes)
        
        # 创建相关性矩阵（简化版，实际应基于真实数据）
        correlation_matrix = pd.DataFrame(
            np.eye(n),  # 对角线为1，其他为0
            index=etf_codes,
            columns=etf_codes
        )
        
        # 添加随机相关性（实际应用中应基于真实数据）
        for i in range(n):
            for j in range(i+1, n):
                # 根据ETF类型调整相关性
                # 例如：同行业ETF相关性更高
                base_corr = 0.5
                correlation_matrix.iloc[i, j] = correlation_matrix.iloc[j, i] = base_corr
        
        return correlation_matrix
    
    except Exception as e:
        logger.error(f"计算相关性矩阵失败: {str(e)}")
        # 返回单位矩阵作为默认值
        etf_codes = etfs["etf_code"].tolist()
        return pd.DataFrame(np.eye(len(etf_codes)), index=etf_codes, columns=etf_codes)

def select_low_correlation_etfs(
    etfs: pd.DataFrame, 
    correlation_matrix: pd.DataFrame,
    max_holdings: int,
    min_correlation: float
) -> pd.DataFrame:
    """
    选择低相关性的ETF
    
    Args:
        etfs: ETF列表
        correlation_matrix: 相关性矩阵
        max_holdings: 最大持有数量
        min_correlation: 最小相关性阈值
    
    Returns:
        pd.DataFrame: 选中的ETF列表
    """
    try:
        # 按评分排序
        etfs = etfs.sort_values("score", ascending=False)
        
        # 选择低相关性的ETF
        selected = []
        for _, etf in etfs.iterrows():
            if not selected:
                selected.append(etf)
                continue
            
            # 检查与已选ETF的相关性
            is_low_correlation = True
            for sel in selected:
                corr = correlation_matrix.loc[etf["etf_code"], sel["etf_code"]]
                if corr > min_correlation:
                    is_low_correlation = False
                    break
            
            if is_low_correlation:
                selected.append(etf)
                if len(selected) >= max_holdings:
                    break
        
        return pd.DataFrame(selected)
    
    except Exception as e:
        logger.error(f"选择低相关性ETF失败: {str(e)}")
        # 返回评分最高的ETF
        return etfs.head(1)

def calculate_risk_parity_weights(etfs: pd.DataFrame) -> list:
    """
    计算风险平价权重（使各ETF对组合风险的贡献相等）
    
    Args:
        etfs: ETF列表（包含波动率等信息）
    
    Returns:
        list: 风险平价权重
    """
    try:
        # 权重与波动率成反比
        volatilities = []
        for _, etf in etfs.iterrows():
            # 获取ETF的波动率（从元数据或计算）
            df = load_etf_daily_data(etf["etf_code"])
            if not df.empty:
                volatility = calculate_volatility(df)
                volatilities.append(volatility)
            else:
                volatilities.append(0.1)  # 默认波动率
        
        weights = 1.0 / np.array(volatilities)
        weights = weights / np.sum(weights)  # 归一化
        
        return weights.tolist()
    
    except Exception as e:
        logger.error(f"计算风险平价权重失败: {str(e)}")
        # 返回等权重
        return [1.0/len(etfs)] * len(etfs)

def calculate_dynamic_stop_loss(etf_code: str, df: pd.DataFrame, position_type: str) -> float:
    """
    计算波动率自适应止损比例
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
        position_type: 仓位类型
    
    Returns:
        float: 动态止损比例
    """
    try:
        # 计算ATR（平均真实波幅）
        atr = calculate_atr(df, 20)
        
        # 计算当前价格
        current_price = df.iloc[-1]["收盘"]
        
        # 计算基于ATR的止损距离
        params = Config.STRATEGY_PARAMETERS[position_type]
        stop_loss_distance = params["stop_loss_ratio"] * current_price
        
        # 考虑市场波动率
        volatility = calculate_volatility(df)
        if volatility > 0.2:  # 高波动率市场
            stop_loss_distance *= 1.2
        elif volatility < 0.1:  # 低波动率市场
            stop_loss_distance *= 0.8
        
        # 转换为百分比
        stop_loss_ratio = stop_loss_distance / current_price
        
        # 设置上下限
        min_ratio = 0.01  # 最小止损比例1%
        max_ratio = 0.15  # 最大止损比例15%
        
        return max(min(stop_loss_ratio, max_ratio), min_ratio)
    
    except Exception as e:
        logger.error(f"计算ETF {etf_code} 动态止损失败: {str(e)}")
        # 返回默认止损比例
        return Config.STRATEGY_PARAMETERS[position_type]["stop_loss_ratio"]

def generate_trading_signals(etf_code: str, df: pd.DataFrame, position_type: str) -> dict:
    """
    生成交易信号（基于多因子确认）
    
    Args:
        etf_code: ETF代码
        df: ETF日线数据
        position_type: 仓位类型
    
    Returns:
        dict: 交易信号（包含信号类型、强度等）
    """
    try:
        # 1. 均线信号
        ma_bullish, ma_bearish = calculate_ma_signal(df, 5, 20)
        
        # 2. MACD信号
        macd_bullish, macd_bearish = calculate_macd_signal(df)
        
        # 3. RSI信号
        rsi_signal = calculate_rsi_signal(df)
        
        # 4. 量能信号
        volume_signal = calculate_volume_signal(df)
        
        # 5. 布林带信号
        bb_signal = calculate_bollinger_bands_signal(df)
        
        # 6. 趋势强度
        trend_strength = calculate_trend_strength(df)
        
        # 综合信号强度
        buy_strength = 0
        sell_strength = 0
        
        # 均线信号
        if ma_bullish:
            buy_strength += 2
        elif ma_bearish:
            sell_strength += 2
        
        # MACD信号
        if macd_bullish:
            buy_strength += 2
        elif macd_bearish:
            sell_strength += 2
        
        # RSI信号（仅在极端区域考虑）
        if rsi_signal == "oversold":
            buy_strength += 1
        elif rsi_signal == "overbought":
            sell_strength += 1
        
        # 量能信号
        if volume_signal == "bullish":
            buy_strength += 1
        elif volume_signal == "bearish":
            sell_strength += 1
        
        # 布林带信号
        if bb_signal == "lower_band":
            buy_strength += 1
        elif bb_signal == "upper_band":
            sell_strength += 1
        
        # 趋势强度
        if trend_strength > 0.7:
            buy_strength += 1
        elif trend_strength < 0.3:
            sell_strength += 1
        
        # 确定最终信号
        signal_type = "none"
        signal_strength = 0
        
        # 根据仓位类型调整信号确认阈值
        params = Config.STRATEGY_PARAMETERS[position_type]
        confirm_threshold = 4 if params["confirm_days"] > 1 else 3
        
        if buy_strength >= confirm_threshold:
            signal_type = "buy"
            signal_strength = buy_strength
        elif sell_strength >= confirm_threshold:
            signal_type = "sell"
            signal_strength = sell_strength
        
        return {
            "signal_type": signal_type,
            "signal_strength": signal_strength,
            "ma_signal": "bullish" if ma_bullish else "bearish" if ma_bearish else "none",
            "macd_signal": "bullish" if macd_bullish else "bearish" if macd_bearish else "none",
            "rsi_signal": rsi_signal,
            "volume_signal": volume_signal,
            "bb_signal": bb_signal,
            "trend_strength": trend_strength
        }
    
    except Exception as e:
        logger.error(f"生成ETF {etf_code} 交易信号失败: {str(e)}")
        return {"signal_type": "none", "signal_strength": 0}

def calculate_ma_signal(df: pd.DataFrame, short_period: int = 5, long_period: int = 20) -> Tuple[bool, bool]:
    """
    计算均线信号
    
    Args:
        df: ETF日线数据
        short_period: 短期均线周期
        long_period: 长期均线周期
    
    Returns:
        Tuple[bool, bool]: (看涨信号, 看跌信号)
    """
    try:
        if len(df) < long_period:
            return False, False
        
        # 计算均线
        df["ma_short"] = df[CLOSE_COL].rolling(short_period).mean()
        df["ma_long"] = df[CLOSE_COL].rolling(long_period).mean()
        
        # 检查最近两天的均线交叉
        if len(df) >= 2:
            prev = df.iloc[-2]
            latest = df.iloc[-1]
            
            # 看涨信号：短期均线从下向上穿越长期均线
            ma_bullish = prev["ma_short"] <= prev["ma_long"] and latest["ma_short"] > latest["ma_long"]
            
            # 看跌信号：短期均线从上向下穿越长期均线
            ma_bearish = prev["ma_short"] >= prev["ma_long"] and latest["ma_short"] < latest["ma_long"]
            
            return ma_bullish, ma_bearish
        
        return False, False
    
    except Exception as e:
        error_msg = f"计算均线信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return False, False

def calculate_macd_signal(df: pd.DataFrame, fast_period: int = 12, slow_period: int = 26, signal_period: int = 9) -> Tuple[bool, bool]:
    """
    计算MACD信号
    
    Args:
        df: ETF日线数据
        fast_period: 快速EMA周期
        slow_period: 慢速EMA周期
        signal_period: 信号线周期
    
    Returns:
        Tuple[bool, bool]: (看涨信号, 看跌信号)
    """
    try:
        if len(df) < slow_period + signal_period:
            return False, False
        
        # 计算MACD
        df["ema_fast"] = df[CLOSE_COL].ewm(span=fast_period, adjust=False).mean()
        df["ema_slow"] = df[CLOSE_COL].ewm(span=slow_period, adjust=False).mean()
        df["macd"] = df["ema_fast"] - df["ema_slow"]
        df["signal"] = df["macd"].ewm(span=signal_period, adjust=False).mean()
        df["hist"] = df["macd"] - df["signal"]
        
        # 检查最近两天的MACD交叉
        if len(df) >= 2:
            prev = df.iloc[-2]
            latest = df.iloc[-1]
            
            # 看涨信号：MACD从下向上穿越信号线
            macd_bullish = prev["macd"] <= prev["signal"] and latest["macd"] > latest["signal"]
            
            # 看跌信号：MACD从上向下穿越信号线
            macd_bearish = prev["macd"] >= prev["signal"] and latest["macd"] < latest["signal"]
            
            return macd_bullish, macd_bearish
        
        return False, False
    
    except Exception as e:
        error_msg = f"计算MACD信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return False, False

def calculate_rsi_signal(df: pd.DataFrame, period: int = 14) -> str:
    """
    计算RSI信号
    
    Args:
        df: ETF日线数据
        period: RSI周期
    
    Returns:
        str: RSI信号（'oversold', 'overbought', 'neutral'）
    """
    try:
        if len(df) < period + 1:
            return "neutral"
        
        # 计算价格变化
        delta = df[CLOSE_COL].diff()
        gain = delta.where(delta > 0, 0)
        loss = -delta.where(delta < 0, 0)
        
        # 计算平均增益和平均损失
        avg_gain = gain.rolling(window=period).mean()
        avg_loss = loss.rolling(window=period).mean()
        
        # 计算RS和RSI
        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))
        
        # 获取最新RSI值
        latest_rsi = rsi.iloc[-1]
        
        # 判断信号
        if latest_rsi < 30:
            return "oversold"  # 超卖
        elif latest_rsi > 70:
            return "overbought"  # 超买
        else:
            return "neutral"  # 中性
    
    except Exception as e:
        error_msg = f"计算RSI信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return "neutral"

def calculate_volume_signal(df: pd.DataFrame, period: int = 20) -> str:
    """
    计算量能信号
    
    Args:
        df: ETF日线数据
        period: 周期
    
    Returns:
        str: 量能信号（'bullish', 'bearish', 'neutral'）
    """
    try:
        if len(df) < period:
            return "neutral"
        
        # 计算平均成交量
        df["avg_volume"] = df[VOLUME_COL].rolling(period).mean()
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 判断量能信号
        if latest[VOLUME_COL] > latest["avg_volume"] * 1.2:
            return "bullish"  # 放量
        elif latest[VOLUME_COL] < latest["avg_volume"] * 0.8:
            return "bearish"  # 缩量
        else:
            return "neutral"  # 正常
    
    except Exception as e:
        error_msg = f"计算量能信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return "neutral"

def calculate_bollinger_bands_signal(df: pd.DataFrame, period: int = 20, num_std: int = 2) -> str:
    """
    计算布林带信号
    
    Args:
        df: ETF日线数据
        period: 周期
        num_std: 标准差倍数
    
    Returns:
        str: 布林带信号（'lower_band', 'upper_band', 'middle_band'）
    """
    try:
        if len(df) < period:
            return "middle_band"
        
        # 计算布林带
        df["ma"] = df[CLOSE_COL].rolling(period).mean()
        df["std"] = df[CLOSE_COL].rolling(period).std()
        df["upper_band"] = df["ma"] + num_std * df["std"]
        df["lower_band"] = df["ma"] - num_std * df["std"]
        
        # 获取最新数据
        latest = df.iloc[-1]
        
        # 判断布林带信号
        if latest[CLOSE_COL] <= latest["lower_band"]:
            return "lower_band"  # 触及下轨
        elif latest[CLOSE_COL] >= latest["upper_band"]:
            return "upper_band"  # 触及上轨
        else:
            return "middle_band"  # 中轨附近
    
    except Exception as e:
        error_msg = f"计算布林带信号失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return "middle_band"

def calculate_trend_strength(df: pd.DataFrame, period: int = 30) -> float:
    """
    计算趋势强度
    
    Args:
        df: ETF日线数据
        period: 周期
    
    Returns:
        float: 趋势强度（0-1）
    """
    try:
        if len(df) < period:
            return 0.5  # 默认中等趋势
        
        # 计算价格变化
        start_price = df[CLOSE_COL].iloc[-period]
        end_price = df[CLOSE_COL].iloc[-1]
        
        # 计算总变化
        total_change = end_price - start_price
        
        # 计算绝对波动
        absolute_volatility = df[CLOSE_COL].diff().abs().sum()
        
        # 趋势强度 = |总变化| / 绝对波动
        trend_strength = abs(total_change) / absolute_volatility if absolute_volatility != 0 else 0
        
        # 限制在0-1范围内
        return min(max(trend_strength, 0), 1)
    
    except Exception as e:
        error_msg = f"计算趋势强度失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        return 0.5

def adjust_position_size(risk_level: str, market_condition: str, volatility: float) -> float:
    """
    根据风险水平、市场环境和波动率动态调整仓位比例
    
    Args:
        risk_level: 当前风险水平（'low', 'medium', 'high'）
        market_condition: 市场环境（'bull', 'bear', 'sideways'）
        volatility: 当前市场波动率
    
    Returns:
        float: 目标仓位比例
    """
    # 基础仓位比例
    base_position = 0.6  # 默认60%仓位
    
    # 风险水平调整
    if risk_level == "high":
        base_position -= 0.3
    elif risk_level == "medium":
        base_position -= 0.1
    
    # 市场环境调整
    if market_condition == "bear":
        base_position -= 0.2
    elif market_condition == "bull":
        base_position += 0.1
    
    # 波动率调整（波动率越高，仓位越低）
    volatility_factor = 1.0 - min(max((volatility - 0.1) * 2, 0), 0.3)
    adjusted_position = base_position * volatility_factor
    
    # 设置上下限
    min_position = 0.2  # 最低20%仓位
    max_position = 0.8  # 最高80%仓位
    
    return max(min(adjusted_position, max_position), min_position)

def monitor_portfolio_risk(positions: pd.DataFrame) -> dict:
    """
    监控投资组合风险
    
    Args:
        positions: 当前投资组合
    
    Returns:
        dict: 风险指标
    """
    try:
        # 获取持仓ETF代码
        etf_codes = []
        for _, position in positions.iterrows():
            if not pd.isna(position["ETF代码"]) and position["ETF代码"]:
                etf_codes.extend([code.strip() for code in str(position["ETF代码"]).split(",") if code.strip()])
        
        if not etf_codes:
            return {
                "portfolio_volatility": 0.0,
                "var_1d": 0.0,
                "max_drawdown_warning": 0.0,
                "liquidity_risk": 0.0,
                "tracking_risk": 0.0,
                "correlation_risk": 0.0,
                "overall_risk_level": "low",
                "risk_alert": "无持仓，风险极低"
            }
        
        # 计算组合波动率
        portfolio_volatility = calculate_portfolio_volatility(etf_codes)
        
        # 计算VaR（在险价值）
        var_1d = calculate_var(etf_codes, 1, 0.95)
        
        # 计算最大回撤预警
        max_drawdown_warning = calculate_max_drawdown_warning(etf_codes)
        
        # 监控流动性风险
        liquidity_risk = monitor_liquidity_risk(etf_codes)
        
        # 监控跟踪误差风险
        tracking_risk = monitor_tracking_risk(etf_codes)
        
        # 监控相关性风险
        correlation_risk = monitor_correlation_risk(etf_codes)
        
        # 综合风险水平
        overall_risk_level = calculate_overall_risk_level(
            portfolio_volatility,
            var_1d,
            max_drawdown_warning,
            liquidity_risk,
            tracking_risk,
            correlation_risk
        )
        
        return {
            "portfolio_volatility": portfolio_volatility,
            "var_1d": var_1d,
            "max_drawdown_warning": max_drawdown_warning,
            "liquidity_risk": liquidity_risk,
            "tracking_risk": tracking_risk,
            "correlation_risk": correlation_risk,
            "overall_risk_level": overall_risk_level,
            "risk_alert": generate_risk_alert(overall_risk_level)
        }
    
    except Exception as e:
        logger.error(f"组合风险监控失败: {str(e)}")
        return {
            "overall_risk_level": "high",
            "error": str(e),
            "risk_alert": "风险监控系统故障，请立即检查！"
        }

def calculate_portfolio_volatility(etf_codes: List[str]) -> float:
    """
    计算投资组合波动率
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        float: 组合波动率
    """
    try:
        if not etf_codes:
            return 0.0
        
        # 获取每只ETF的波动率
        volatilities = []
        for code in etf_codes:
            df = load_etf_daily_data(code)
            if not df.empty:
                volatility = calculate_volatility(df)
                volatilities.append(volatility)
        
        if not volatilities:
            return 0.1  # 默认波动率
        
        # 计算加权平均波动率（简化版）
        return sum(volatilities) / len(volatilities)
    
    except Exception as e:
        logger.error(f"计算组合波动率失败: {str(e)}")
        return 0.15  # 默认波动率

def calculate_var(etf_codes: List[str], days: int = 1, confidence: float = 0.95) -> float:
    """
    计算组合的VaR（在险价值）
    
    Args:
        etf_codes: ETF代码列表
        days: 预测天数
        confidence: 置信水平
    
    Returns:
        float: VaR值
    """
    try:
        if not etf_codes:
            return 0.0
        
        # 这里简化处理，实际应使用历史模拟法或参数法计算VaR
        # 假设组合波动率为15%，使用正态分布计算
        portfolio_volatility = calculate_portfolio_volatility(etf_codes)
        z_score = 1.645  # 95%置信水平的Z值
        
        # 计算1天VaR
        var_1d = portfolio_volatility * z_score
        
        # 调整为指定天数
        var = var_1d * np.sqrt(days)
        
        return var
    
    except Exception as e:
        logger.error(f"计算VaR失败: {str(e)}")
        return 0.05  # 默认VaR

def calculate_max_drawdown_warning(etf_codes: List[str]) -> float:
    """
    计算组合最大回撤预警
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        float: 最大回撤预警值
    """
    try:
        if not etf_codes:
            return 0.0
        
        # 计算每只ETF的最大回撤
        max_drawdowns = []
        for code in etf_codes:
            df = load_etf_daily_data(code)
            if not df.empty:
                max_drawdown = calculate_max_drawdown(df)
                max_drawdowns.append(max_drawdown)
        
        if not max_drawdowns:
            return 0.1  # 默认最大回撤
        
        # 计算加权平均最大回撤
        return sum(max_drawdowns) / len(max_drawdowns)
    
    except Exception as e:
        logger.error(f"计算最大回撤预警失败: {str(e)}")
        return 0.15  # 默认最大回撤

def monitor_liquidity_risk(etf_codes: List[str]) -> float:
    """
    监控组合流动性风险
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        float: 流动性风险值
    """
    try:
        if not etf_codes:
            return 0.0
        
        # 计算每只ETF的流动性评分
        liquidity_scores = []
        for code in etf_codes:
            df = load_etf_daily_data(code)
            if not df.empty:
                liquidity_score = calculate_liquidity_score(df)
                liquidity_scores.append(liquidity_score)
        
        if not liquidity_scores:
            return 0.5  # 默认流动性风险
        
        # 将流动性评分转换为风险值（评分越高，风险越低）
        liquidity_risk = 1.0 - (sum(liquidity_scores) / len(liquidity_scores) / 100)
        
        return liquidity_risk
    
    except Exception as e:
        logger.error(f"监控流动性风险失败: {str(e)}")
        return 0.5  # 默认流动性风险

def monitor_tracking_risk(etf_codes: List[str]) -> float:
    """
    监控组合跟踪误差风险
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        float: 跟踪误差风险值
    """
    try:
        if not etf_codes:
            return 0.0
        
        # 计算每只ETF的跟踪误差
        tracking_errors = []
        for code in etf_codes:
            df = load_etf_daily_data(code)
            if not df.empty:
                tracking_error = calculate_tracking_error(df)
                tracking_errors.append(tracking_error)
        
        if not tracking_errors:
            return 0.05  # 默认跟踪误差
        
        # 计算加权平均跟踪误差
        return sum(tracking_errors) / len(tracking_errors)
    
    except Exception as e:
        logger.error(f"监控跟踪误差风险失败: {str(e)}")
        return 0.08  # 默认跟踪误差

def monitor_correlation_risk(etf_codes: List[str]) -> float:
    """
    监控组合相关性风险
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        float: 相关性风险值
    """
    try:
        if len(etf_codes) < 2:
            return 0.0  # 单只ETF没有相关性风险
        
        # 计算ETF之间的相关性矩阵
        correlation_matrix = calculate_correlation_matrix_for_codes(etf_codes)
        
        # 计算平均相关性
        n = len(etf_codes)
        total_corr = 0.0
        count = 0
        
        for i in range(n):
            for j in range(i+1, n):
                total_corr += correlation_matrix.iloc[i, j]
                count += 1
        
        if count == 0:
            return 0.0
        
        avg_corr = total_corr / count
        
        # 相关性越高，风险越大
        return avg_corr
    
    except Exception as e:
        logger.error(f"监控相关性风险失败: {str(e)}")
        return 0.6  # 默认相关性

def calculate_correlation_matrix_for_codes(etf_codes: List[str]) -> pd.DataFrame:
    """
    计算指定ETF代码之间的相关性矩阵
    
    Args:
        etf_codes: ETF代码列表
    
    Returns:
        pd.DataFrame: 相关性矩阵
    """
    try:
        n = len(etf_codes)
        
        # 创建相关性矩阵（简化版）
        correlation_matrix = pd.DataFrame(
            np.eye(n),
            index=etf_codes,
            columns=etf_codes
        )
        
        # 添加随机相关性
        for i in range(n):
            for j in range(i+1, n):
                # 根据ETF类型调整相关性
                base_corr = 0.5
                correlation_matrix.iloc[i, j] = correlation_matrix.iloc[j, i] = base_corr
        
        return correlation_matrix
    
    except Exception as e:
        logger.error(f"计算相关性矩阵失败: {str(e)}")
        # 返回单位矩阵作为默认值
        return pd.DataFrame(np.eye(len(etf_codes)), index=etf_codes, columns=etf_codes)

def calculate_overall_risk_level(*risk_metrics) -> str:
    """
    计算综合风险水平
    
    Args:
        *risk_metrics: 风险指标
    
    Returns:
        str: 风险水平（'low', 'medium', 'high'）
    """
    # 将风险指标转换为风险分数（0-1）
    def metric_to_score(metric):
        if isinstance(metric, (int, float)):
            return min(max(metric, 0), 1)
        return 0.5  # 默认中等风险
    
    risk_scores = [metric_to_score(m) for m in risk_metrics]
    
    # 计算加权平均风险分数
    weights = [0.2, 0.2, 0.2, 0.15, 0.15, 0.1]  # 根据重要性分配权重
    weighted_avg = sum(s * w for s, w in zip(risk_scores, weights))
    
    # 确定风险等级
    if weighted_avg > 0.7:
        return "high"
    elif weighted_avg > 0.4:
        return "medium"
    else:
        return "low"

def generate_risk_alert(risk_level: str) -> str:
    """
    生成风险预警信息
    
    Args:
        risk_level: 风险水平
    
    Returns:
        str: 风险预警信息
    """
    if risk_level == "high":
        return "⚠️ 高风险预警：建议大幅降低仓位或切换至防御性策略！"
    elif risk_level == "medium":
        return "⚠️ 中等风险：建议适当降低仓位或调整持仓结构！"
    else:
        return "✅ 风险水平正常：可维持当前仓位策略。"

def evaluate_opportunity_pool(market_condition: str, risk_level: dict) -> str:
    """
    评估机会池中的ETF
    
    Args:
        market_condition: 市场环境
        risk_level: 风险水平
    
    Returns:
        str: 机会池评估建议
    """
    try:
        # 加载机会池
        opportunity_pool = load_opportunity_pool()
        
        if opportunity_pool.empty:
            return "• 机会池中无ETF，无需关注"
        
        # 按机会评分排序
        opportunity_pool = opportunity_pool.sort_values("机会评分", ascending=False)
        
        # 获取评分前3的机会
        top_opportunities = opportunity_pool.head(3)
        
        # 生成建议
        suggestions = ["【机会池评估】"]
        suggestions.append("• 重点关注以下接近达标但有潜力的ETF：")
        
        for _, opportunity in top_opportunities.iterrows():
            suggestions.append(f"  - {opportunity['ETF代码']} {opportunity['ETF名称']} "
                              f"(评分: {opportunity['机会评分']:.1f}/100, "
                              f"规模: {opportunity['基金规模']:.2f}亿, "
                              f"流动性: {opportunity['日均成交额']:.2f}万)")
        
        suggestions.append("• 建议持续关注，当规模或流动性达标时可考虑纳入投资组合")
        
        return "\n".join(suggestions)
    
    except Exception as e:
        logger.error(f"机会池评估失败: {str(e)}", exc_info=True)
        return "• 机会池评估失败，请检查系统日志"

def load_opportunity_pool() -> pd.DataFrame:
    """
    加载机会池
    
    Returns:
        pd.DataFrame: 机会池数据
    """
    try:
        opportunity_pool_path = os.path.join(Config.DATA_DIR, "opportunity_pool.csv")
        if not os.path.exists(opportunity_pool_path):
            return pd.DataFrame()
        
        return pd.read_csv(opportunity_pool_path, encoding="utf-8-sig")
    
    except Exception as e:
        logger.error(f"加载机会池失败: {str(e)}", exc_info=True)
        return pd.DataFrame()

def get_etf_score(etf_code: str) -> float:
    """
    获取ETF评分
    
    Args:
        etf_code: ETF代码
    
    Returns:
        float: ETF评分
    """
    try:
        # 获取评分结果
        top_etfs = get_top_rated_etfs(top_n=100)
        if not top_etfs.empty:
            etf_row = top_etfs[top_etfs["etf_code"] == etf_code]
            if not etf_row.empty:
                return etf_row.iloc[0]["score"]
        
        # 如果没有评分，返回默认值
        return 60.0
    
    except Exception as e:
        logger.error(f"获取ETF {etf_code} 评分失败: {str(e)}")
        return 50.0

def record_trade_actions(trade_actions: List[Dict]):
    """
    记录交易动作
    
    Args:
        trade_actions: 交易动作列表
    """
    try:
        # 确保目录存在
        os.makedirs(os.path.dirname(TRADE_RECORD_PATH), exist_ok=True)
        
        # 检查文件是否存在
        if os.path.exists(TRADE_RECORD_PATH):
            # 读取现有记录
            trade_df = pd.read_csv(TRADE_RECORD_PATH, encoding="utf-8")
        else:
            # 创建新的DataFrame
            columns = [
                "时间(UTC)", "时间(北京时间)", "持仓类型", "ETF代码", "ETF名称",
                "价格", "数量", "操作", "备注"
            ]
            trade_df = pd.DataFrame(columns=columns)
        
        # 添加新交易记录
        new_records = pd.DataFrame(trade_actions)
        trade_df = pd.concat([trade_df, new_records], ignore_index=True)
        
        # 保存记录
        trade_df.to_csv(TRADE_RECORD_PATH, index=False, encoding="utf-8-sig")
        logger.info(f"已记录 {len(trade_actions)} 笔交易动作")
    
    except Exception as e:
        error_msg = f"记录交易动作失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)

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
        error_msg = f"保存风险监控记录失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)

# 模块初始化
try:
    logger.info("仓位管理模块初始化完成")
    
    # 检查ETF列表是否过期
    if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
        warning_msg = "ETF列表已过期，评分系统可能使用旧数据"
        logger.warning(warning_msg)
        
        # 发送警告通知
        try:
            from wechat_push import send_wechat_message
            send_wechat_message(
                message=warning_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
    
    # 初始化仓位记录
    init_position_record()
    
except Exception as e:
    error_msg = f"仓位管理模块初始化失败: {str(e)}"
    logger.error(error_msg, exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(error_msg)
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(error_msg)
    
    # 发送错误通知
    try:
        from wechat_push import send_wechat_message
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)