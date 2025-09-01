#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Karmy-Gold ETF量化策略系统
主入口文件，负责调度不同任务类型
"""

import os
import sys
import json
import logging
import traceback
from datetime import datetime
from typing import Dict, Any, Optional
from config import Config
from etf_scoring import get_top_rated_etfs
from position import calculate_position_strategy
from wechat_push import send_wechat_message, send_multiple_messages
from etf_list import update_all_etf_list
from date_utils import (
    get_current_times,
    get_beijing_time,
    is_file_outdated,
    is_trading_day
)
from data_cleaner import clean_all

# 初始化日志
Config.setup_logging(log_file=Config.LOG_FILE)
logger = logging.getLogger(__name__)

def is_manual_trigger() -> bool:
    """
    检查是否为手动触发任务
    
    Returns:
        bool: 如果是手动触发返回True，否则返回False
    """
    try:
        # GitHub Actions手动触发事件名称
        event_name = os.getenv("GITHUB_EVENT_NAME", "")
        return event_name == "workflow_dispatch"
    except Exception as e:
        logger.error(f"检查触发方式失败: {str(e)}", exc_info=True)
        # 出错时保守策略：认为不是手动触发
        return False

def should_execute_crawl_etf_daily() -> bool:
    """
    判断是否应该执行ETF日线数据爬取任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行ETF日线数据爬取")
        return True
    
    # 定时触发的任务：检查是否是交易日或是否已过18点
    beijing_time = get_beijing_time()
    beijing_date = beijing_time.date()
    
    # 非交易日且未到补爬时间（18点后允许补爬）
    if not is_trading_day(beijing_date) and beijing_time.hour < 18:
        logger.info(f"今日{beijing_date}非交易日且未到补爬时间（{beijing_time.hour}点），跳过爬取日线数据（定时任务）")
        return False
    
    return True

def should_execute_calculate_arbitrage() -> bool:
    """
    判断是否应该执行套利机会计算任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行套利机会计算")
        return True
    
    # 定时触发的任务：检查当天是否已推送
    if check_flag(Config.get_arbitrage_flag_file()):
        logger.info("今日已推送套利机会，跳过本次计算（定时任务）")
        return False
    
    return True

def should_execute_calculate_position() -> bool:
    """
    判断是否应该执行仓位策略计算任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行仓位策略计算")
        return True
    
    # 定时触发的任务：检查当天是否已推送
    if check_flag(Config.get_position_flag_file()):
        logger.info("今日已推送仓位策略，跳过本次计算（定时任务）")
        return False
    
    return True

def should_execute_clean_data() -> bool:
    """
    判断是否应该执行数据清理任务
    
    Returns:
        bool: 如果应该执行返回True，否则返回False
    """
    # 手动触发的任务总是执行
    if is_manual_trigger():
        logger.info("手动触发的任务，总是执行数据清理")
        return True
    
    # 定时触发的任务：每天凌晨1点执行
    beijing_time = get_beijing_time()
    if beijing_time.hour == 1 and beijing_time.minute < 30:
        return True
    
    return False

def check_flag(flag_file: str) -> bool:
    """
    检查标记文件是否存在
    
    Args:
        flag_file: 标记文件路径
    
    Returns:
        bool: 如果标记文件存在返回True，否则返回False
    """
    return os.path.exists(flag_file)

def set_flag(flag_file: str) -> bool:
    """
    设置标记文件
    
    Args:
        flag_file: 标记文件路径
    
    Returns:
        bool: 设置成功返回True，否则返回False
    """
    try:
        os.makedirs(os.path.dirname(flag_file), exist_ok=True)
        with open(flag_file, 'w') as f:
            f.write(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        return True
    except Exception as e:
        logger.error(f"设置标记文件失败: {str(e)}", exc_info=True)
        return False

def handle_update_etf_list() -> Dict[str, Any]:
    """
    处理ETF列表更新任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_update_etf_list():
            logger.info("根据定时任务规则，跳过ETF列表更新任务")
            return {"status": "skipped", "message": "ETF列表未到更新周期"}
        
        logger.info("开始更新全市场ETF列表")
        etf_list = update_all_etf_list()
        
        if etf_list.empty:
            error_msg = "ETF列表更新失败：获取到空的ETF列表"
            logger.error(error_msg)
            result = {"status": "error", "message": error_msg}
            send_task_completion_notification("update_etf_list", result)
            return result
        
        # 确定数据来源
        source = "兜底文件"
        if hasattr(etf_list, 'source'):
            source = etf_list.source
        elif len(etf_list) > 500:  # 假设兜底文件约520只
            source = "网络数据源"
        
        success_msg = f"全市场ETF列表更新完成，共{len(etf_list)}只"
        logger.info(success_msg)
        
        # 获取文件修改时间（UTC与北京时间）
        utc_mtime, beijing_mtime = get_file_mtime(Config.ALL_ETFS_PATH)
        
        # 计算过期时间
        expiration_utc = utc_mtime + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
        expiration_beijing = beijing_mtime + timedelta(days=Config.ETF_LIST_UPDATE_INTERVAL)
        
        # 构建结果字典（包含双时区信息）
        result = {
            "status": "success", 
            "message": success_msg, 
            "count": len(etf_list),
            "source": source,
            "last_modified_utc": utc_mtime.strftime("%Y-%m-%d %H:%M"),
            "last_modified_beijing": beijing_mtime.strftime("%Y-%m-%d %H:%M"),
            "expiration_utc": expiration_utc.strftime("%Y-%m-%d %H:%M"),
            "expiration_beijing": expiration_beijing.strftime("%Y-%m-%d %H:%M")
        }
        
        # 发送任务完成通知
        send_task_completion_notification("update_etf_list", result)
        
        return result
    
    except Exception as e:
        error_msg = f"ETF列表更新失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        result = {"status": "error", "message": error_msg}
        send_task_completion_notification("update_etf_list", result)
        return result

def handle_calculate_position() -> Dict[str, Any]:
    """
    处理仓位策略计算任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_calculate_position():
            logger.info("根据定时任务规则，跳过仓位策略计算任务")
            return {"status": "skipped", "message": "Position strategy already pushed today"}
        
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 计算仓位策略
        logger.info("开始计算仓位策略")
        messages = calculate_position_strategy()
        
        # 推送消息
        send_success = send_multiple_messages(messages, message_type="position")
        
        if send_success:
            set_flag(Config.get_position_flag_file())  # 标记已推送
            return {
                "status": "success", 
                "message": "Position strategy pushed successfully",
                "calculation_time_utc": utc_now.strftime("%Y-%m-%d %H:%M"),
                "calculation_time_beijing": beijing_now.strftime("%Y-%m-%d %H:%M")
            }
        else:
            error_msg = "仓位策略推送失败"
            logger.error(error_msg)
            return {"status": "failed", "message": error_msg}
            
    except Exception as e:
        error_msg = f"仓位策略计算失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 发送错误通知
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def handle_clean_data() -> Dict[str, Any]:
    """
    处理数据清理任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 检查是否应该执行任务（仅对定时任务有效）
        if not is_manual_trigger() and not should_execute_clean_data():
            logger.info("根据定时任务规则，跳过数据清理任务")
            return {"status": "skipped", "message": "Data cleaning not scheduled for this time"}
        
        logger.info("开始执行数据清理任务")
        result = clean_all()
        
        # 发送清理结果通知
        if result["status"] == "success" or result["status"] == "partial_success":
            message = "✅ 数据清理任务执行成功\n"
            message += f"• ETF数据清理: {result['results']['etf_data'].get('cleaned_count', 0)} 个文件\n"
            message += f"• 日志文件清理: {result['results']['log_files'].get('cleaned_count', 0)} 个文件\n"
            message += f"• 临时文件清理: {result['results']['temp_files'].get('cleaned_count', 0)} 个项目"
        else:
            message = "❌ 数据清理任务执行失败\n"
            message += f"• 错误: {result.get('message', '未知错误')}"
        
        # 添加时间戳
        beijing_now = get_beijing_time()
        message += f"\n\n🕒 清理时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}"
        
        # 发送通知
        send_wechat_message(message, message_type="task_notification")
        
        return result
    
    except Exception as e:
        error_msg = f"数据清理任务执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        send_wechat_message(
            message=error_msg,
            message_type="error"
        )
        return {"status": "error", "message": error_msg}

def get_file_mtime(file_path: str) -> Tuple[datetime, datetime]:
    """
    获取文件的最后修改时间（UTC和北京时间）
    
    Args:
        file_path: 文件路径
    
    Returns:
        Tuple[datetime, datetime]: (UTC时间, 北京时间)
    """
    if not os.path.exists(file_path):
        return datetime.now(), datetime.now()
    
    # 获取文件修改时间（本地时间）
    mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
    
    # 转换为UTC时间
    utc_mtime = mtime.replace(tzinfo=Config.BEIJING_TIMEZONE).astimezone(Config.UTC_TIMEZONE)
    
    # 转换为北京时间
    beijing_mtime = mtime.replace(tzinfo=Config.BEIJING_TIMEZONE)
    
    return utc_mtime, beijing_mtime

def send_task_completion_notification(task: str, result: Dict[str, Any]):
    """
    发送任务完成通知
    
    Args:
        task: 任务名称
        result: 任务结果
    """
    if result["status"] == "success":
        message = f"✅ {task} 任务执行成功\n"
        message += f"• 消息: {result['message']}\n"
        if "count" in result:
            message += f"• 数量: {result['count']}只\n"
        if "last_modified_beijing" in result:
            message += f"• 更新时间: {result['last_modified_beijing']}\n"
        if "expiration_beijing" in result:
            message += f"• 过期时间: {result['expiration_beijing']}"
    else:
        message = f"❌ {task} 任务执行失败\n"
        message += f"• 错误: {result['message']}"
    
    # 添加时间戳
    beijing_now = get_beijing_time()
    message += f"\n\n🕒 通知时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}"
    
    # 发送通知
    send_wechat_message(message, message_type="task_notification")

def main() -> Dict[str, Any]:
    """
    主函数：根据环境变量执行对应任务
    
    Returns:
        Dict[str, Any]: 任务执行结果
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        # 从环境变量获取任务类型（由GitHub Actions传递）
        task = os.getenv("TASK", "unknown")
        
        logger.info(f"===== 开始执行任务：{task} =====")
        logger.info(f"UTC时间：{utc_now.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"北京时间：{beijing_now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # 设置环境
        if not setup_environment():
            error_msg = "环境设置失败，任务终止"
            logger.error(error_msg)
            return {"status": "error", "task": task, "message": error_msg}
        
        # 根据任务类型执行对应操作
        task_handlers = {
            "calculate_position": handle_calculate_position,
            "update_etf_list": handle_update_etf_list,
            "clean_data": handle_clean_data
        }
        
        if task in task_handlers:
            result = task_handlers[task]()
            response = {
                "status": result["status"], 
                "task": task, 
                "message": result.get("message", ""),
                "timestamp": beijing_now.isoformat()
            }
        else:
            # 未知任务
            error_msg = (
                f"未知任务类型：{task}（支持的任务："
                f"{', '.join(task_handlers.keys())}）"
            )
            logger.error(error_msg)
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
            response = {"status": "error", "task": task, "message": error_msg}
        
        logger.info(f"===== 任务执行结束：{response['status']} =====")
        
        # 输出JSON格式的结果（供GitHub Actions等调用方使用）
        print(json.dumps(response, indent=2, ensure_ascii=False))
        
        return response
        
    except Exception as e:
        error_msg = f"主程序执行失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        
        # 尝试发送错误消息
        try:
            send_wechat_message(
                message=error_msg,
                message_type="error"
            )
        except Exception as wechat_e:
            logger.error(f"发送微信错误消息失败: {str(wechat_e)}", exc_info=True)
        
        # 返回错误响应
        response = {
            "status": "error",
            "task": os.getenv("TASK", "unknown"),
            "message": error_msg,
            "timestamp": get_beijing_time().isoformat()
        }
        
        print(json.dumps(response, indent=2, ensure_ascii=False))
        return response

def setup_environment() -> bool:
    """
    设置运行环境，检查必要的目录和文件
    
    Returns:
        bool: 环境设置是否成功
    """
    try:
        # 获取当前双时区时间
        utc_now, beijing_now = get_current_times()
        
        logger.info(f"开始设置运行环境 (UTC: {utc_now}, CST: {beijing_now})")
        
        # 确保必要的目录存在
        os.makedirs(Config.DATA_DIR, exist_ok=True)
        os.makedirs(Config.LOG_DIR, exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_arbitrage_flag_file()), exist_ok=True)
        os.makedirs(os.path.dirname(Config.get_position_flag_file()), exist_ok=True)
        
        # 检查ETF列表是否过期
        if os.path.exists(Config.ALL_ETFS_PATH):
            if is_file_outdated(Config.ALL_ETFS_PATH, Config.ETF_LIST_UPDATE_INTERVAL):
                logger.warning("ETF列表已过期，建议更新")
            else:
                logger.info("ETF列表有效")
        else:
            logger.warning("ETF列表文件不存在")
        
        # 检查企业微信配置
        if not Config.WECOM_WEBHOOK:
            logger.warning("企业微信Webhook未配置，消息推送将不可用")
        
        # 记录环境信息
        logger.info(f"当前北京时间: {beijing_now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        logger.info("环境设置完成")
        return True
    except Exception as e:
        error_msg = f"环境设置失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

if __name__ == "__main__":
    # 正常执行
    main()