#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
企业微信推送模块
负责将策略结果推送到企业微信
统一处理消息格式化、分页、重试等机制
"""

import os
import requests
import time
import logging
import json
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from config import Config

# 初始化日志
logger = logging.getLogger(__name__)

# 消息发送频率控制
_last_send_time = 0
_MIN_SEND_INTERVAL = 1.0  # 最小发送间隔(秒)，避免消息过密被封
_MAX_MESSAGE_LENGTH = 2048  # 企业微信消息最大长度(字符)
_MESSAGE_CHUNK_SIZE = 1800  # 消息分块大小(字符)

# 发送失败重试配置
_MAX_RETRIES = 3
_RETRY_DELAYS = [1, 3, 5]  # 重试延迟(秒)

def _rate_limit() -> None:
    """
    速率限制：确保消息发送间隔不低于最小间隔
    """
    global _last_send_time
    current_time = time.time()
    elapsed = current_time - _last_send_time
    
    if elapsed < _MIN_SEND_INTERVAL:
        sleep_time = _MIN_SEND_INTERVAL - elapsed
        time.sleep(sleep_time)
    
    _last_send_time = time.time()

def _check_message_length(message: str) -> List[str]:
    """
    检查消息长度并进行分片处理
    
    Args:
        message: 原始消息
    
    Returns:
        List[str]: 分片后的消息列表
    """
    if len(message) <= _MAX_MESSAGE_LENGTH:
        return [message]
    
    # 按行分割消息，避免在句子中间断开
    lines = message.split('\n')
    chunks = []
    current_chunk = []
    
    for line in lines:
        # 检查添加当前行是否会超过最大长度
        if sum(len(l) + 1 for l in current_chunk) + len(line) + 1 > _MESSAGE_CHUNK_SIZE:
            if current_chunk:
                chunks.append('\n'.join(current_chunk))
                current_chunk = []
        
        current_chunk.append(line)
    
    if current_chunk:
        chunks.append('\n'.join(current_chunk))
    
    return chunks

def _send_message_to_wechat(message: str, webhook: str, retry_count: int = 0) -> bool:
    """
    实际发送消息到企业微信
    
    Args:
        message: 消息内容
        webhook: 企业微信Webhook地址
        retry_count: 重试次数
    
    Returns:
        bool: 是否成功发送
    """
    try:
        # 速率限制
        _rate_limit()
        
        # 构建请求数据
        data = {
            "msgtype": "text",
            "text": {
                "content": message,
                "mentioned_list": ["@all"]
            }
        }
        
        # 发送请求
        headers = {"Content-Type": "application/json"}
        response = requests.post(webhook, json=data, headers=headers, timeout=10)
        
        # 检查响应
        if response.status_code == 200:
            result = response.json()
            if result.get("errcode") == 0:
                logger.info("微信消息发送成功")
                return True
            else:
                error_msg = f"企业微信API错误: {result.get('errmsg', '未知错误')}"
                logger.error(error_msg)
                
                # 如果是频率限制，等待更长时间
                if result.get("errcode") == 429:
                    logger.warning("消息发送频率过高，等待30秒后重试")
                    time.sleep(30)
                    return False
                
                # 如果是无效的webhook，直接返回错误
                if result.get("errcode") == 40037:
                    logger.error("企业微信Webhook无效，请检查配置")
                    return False
        else:
            logger.error(f"请求失败，状态码: {response.status_code}")
        
        return False
    
    except requests.exceptions.RequestException as e:
        logger.error(f"网络请求异常: {str(e)} (重试 {retry_count})")
        return False
    except Exception as e:
        logger.error(f"发送消息时发生未预期错误: {str(e)} (重试 {retry_count})")
        return False

def send_wechat_message(message: str, message_type: str = "default", webhook: Optional[str] = None) -> bool:
    """
    发送消息到企业微信，自动添加固定末尾，支持消息分页和重试机制
    
    Args:
        message: 消息内容
        message_type: 消息类型（"arbitrage", "position", "daily_report", "error", "task_notification"）
        webhook: 企业微信Webhook地址，如果为None则使用配置中的地址
    
    Returns:
        bool: 是否成功发送
    """
    try:
        # 从环境变量获取Webhook（优先于配置文件）
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        if not webhook:
            logger.error("企业微信Webhook未配置，无法发送消息")
            return False
        
        # 格式化消息
        formatted_message = format_message(message, message_type)
        
        # 检查消息长度并进行分片
        message_chunks = _check_message_length(formatted_message)
        
        # 发送每条分片消息
        all_sent = True
        for i, chunk in enumerate(message_chunks):
            # 添加分页标识（如果有多条）
            if len(message_chunks) > 1:
                chunk = f"-------------  第{i+1}条  /  共{len(message_chunks)}条  ---------------------\n\n{chunk}"
            
            # 发送消息并处理重试
            success = False
            for retry_count in range(_MAX_RETRIES):
                if _send_message_to_wechat(chunk, webhook, retry_count):
                    success = True
                    break
                
                # 如果需要重试，等待指定时间
                if retry_count < _MAX_RETRIES - 1:
                    delay = _RETRY_DELAYS[retry_count]
                    logger.warning(f"消息发送失败，{delay}秒后重试 ({retry_count+1}/{_MAX_RETRIES})")
                    time.sleep(delay)
            
            if not success:
                all_sent = False
                logger.error(f"消息分片 {i+1} 发送失败")
        
        return all_sent
    
    except Exception as e:
        error_msg = f"发送微信消息失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return False

def format_message(message: str, message_type: str) -> str:
    """
    格式化消息内容
    
    Args:
        message: 原始消息
        message_type: 消息类型
    
    Returns:
        str: 格式化后的消息
    """
    # 获取当前北京时间
    beijing_now = get_beijing_time().strftime("%Y-%m-%d %H:%M:%S")
    
    # 添加消息类型标识
    if message_type == "arbitrage":
        prefix = "📊 【套利机会】\n"
    elif message_type == "position":
        prefix = "📈 【仓位策略】\n"
    elif message_type == "daily_report":
        prefix = "🗞️ 【每日报告】\n"
    elif message_type == "error":
        prefix = "❌ 【系统错误】\n"
    elif message_type == "task_notification":
        prefix = "🔧 【任务通知】\n"
    elif message_type == "data_cleaning":
        prefix = "🧹 【数据清理】\n"
    else:
        prefix = "ℹ️ 【消息】\n"
    
    # 添加固定页脚
    footer = Config.WECOM_MESFOOTER.format(current_time=beijing_now)
    
    # 组合消息
    return f"{prefix}{message}{footer}"

def send_multiple_messages(messages: List[str], message_type: str = "position") -> bool:
    """
    发送多条消息（自动处理分页）
    
    Args:
        messages: 消息列表（纯业务内容）
        message_type: 消息类型
    
    Returns:
        bool: 是否全部发送成功
    """
    if not messages:
        logger.warning("没有消息需要发送")
        return False
    
    all_sent = True
    total_messages = len(messages)
    
    for i, message in enumerate(messages):
        # 格式化单条消息（添加分页标识）
        page_indicator = f"-------------  第{i+1}条  /  共{total_messages}条  ---------------------\n\n"
        formatted_message = f"{page_indicator}{message}"
        
        # 发送消息
        success = send_wechat_message(formatted_message, message_type)
        if not success:
            all_sent = False
    
    return all_sent

def get_beijing_time() -> datetime:
    """
    获取当前北京时间
    
    Returns:
        datetime: 北京时间
    """
    try:
        # 尝试从config获取时区
        from config import Config
        return datetime.now(Config.BEIJING_TIMEZONE)
    except:
        # 保守方案：返回本地时间
        return datetime.now()

def test_webhook_connection(webhook: Optional[str] = None) -> bool:
    """
    测试企业微信Webhook连接
    
    Args:
        webhook: 企业微信Webhook地址
    
    Returns:
        bool: 连接是否成功
    """
    try:
        # 从环境变量获取Webhook（优先于配置文件）
        if webhook is None:
            webhook = os.getenv("WECOM_WEBHOOK", Config.WECOM_WEBHOOK)
        if not webhook:
            logger.error("企业微信Webhook未配置")
            return False
        
        # 发送测试消息
        test_message = "【测试消息】企业微信Webhook连接测试成功\n"
        test_message += f"发送时间: {get_beijing_time().strftime('%Y-%m-%d %H:%M:%S')}"
        
        return send_wechat_message(test_message, "task_notification", webhook)
    
    except Exception as e:
        logger.error(f"测试Webhook连接时发生错误: {str(e)}")
        return False

# 模块初始化
try:
    logger.info("微信推送模块初始化完成")
    
    # 测试Webhook连接（仅在调试模式下）
    if os.getenv("DEBUG", "").lower() in ("true", "1", "yes"):
        logger.info("调试模式启用，测试Webhook连接")
        if test_webhook_connection():
            logger.info("Webhook连接测试成功")
        else:
            logger.warning("Webhook连接测试失败")
    
except Exception as e:
    logger.error(f"微信推送模块初始化失败: {str(e)}", exc_info=True)
    
    try:
        # 退回到基础日志配置
        import logging
        logging.basicConfig(
            level="INFO",
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[logging.StreamHandler()]
        )
        logging.error(f"微信推送模块初始化失败: {str(e)}")
    except Exception as basic_log_error:
        print(f"基础日志配置失败: {str(basic_log_error)}")
        print(f"微信推送模块初始化失败: {str(e)}")
    
    # 尝试发送错误通知
    try:
        # 简单的错误通知，不依赖其他模块
        webhook = os.getenv("WECOM_WEBHOOK", "")
        if webhook:
            requests.post(webhook, json={
                "msgtype": "text",
                "text": {
                    "content": f"【系统错误】微信推送模块初始化失败: {str(e)}"
                }
            })
    except Exception as send_error:
        logger.error(f"发送错误通知失败: {str(send_error)}", exc_info=True)