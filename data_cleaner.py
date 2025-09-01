#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
数据清理模块
负责清理过期数据，包括：
- ETF日线数据（只保留最近365天）
- 日志文件（只保留最近7天）
- 临时文件和错误记录（只保留最近7天）
- 交易流水数据永久保存（不清理）
"""

import os
import logging
import shutil
import datetime
from datetime import timedelta
from config import Config
from date_utils import get_beijing_time

# 初始化日志
logger = logging.getLogger(__name__)

def clean_etf_data():
    """
    清理ETF日线数据，只保留最近365天的数据
    """
    try:
        logger.info("开始清理ETF日线数据...")
        
        # 获取当前日期
        current_date = get_beijing_time().date()
        
        # 计算保留数据的起始日期（1年前）
        cutoff_date = current_date - timedelta(days=365)
        logger.info(f"ETF数据保留策略：只保留 {cutoff_date} 之后的数据")
        
        # 遍历ETF日线数据目录
        cleaned_count = 0
        for filename in os.listdir(Config.ETFS_DAILY_DIR):
            if not filename.endswith('.csv'):
                continue
                
            etf_code = filename.replace('.csv', '')
            file_path = os.path.join(Config.ETFS_DAILY_DIR, filename)
            
            try:
                # 读取ETF数据
                df = pd.read_csv(file_path, encoding='utf-8')
                
                # 检查是否有日期列
                if '日期' not in df.columns:
                    logger.warning(f"ETF {etf_code} 数据缺少日期列，跳过清理")
                    continue
                
                # 转换日期列
                df['日期'] = pd.to_datetime(df['日期']).dt.date
                
                # 过滤保留日期之后的数据
                filtered_df = df[df['日期'] >= cutoff_date]
                
                # 如果数据为空，删除文件
                if filtered_df.empty:
                    os.remove(file_path)
                    logger.info(f"ETF {etf_code} 所有数据已过期，文件已删除")
                    cleaned_count += 1
                # 如果还有数据，保存过滤后的数据
                elif len(filtered_df) < len(df):
                    filtered_df.to_csv(file_path, index=False, encoding='utf-8-sig')
                    logger.info(f"ETF {etf_code} 数据清理完成，保留 {len(filtered_df)} 条记录")
                    cleaned_count += 1
                
            except Exception as e:
                logger.error(f"清理ETF {etf_code} 数据失败: {str(e)}", exc_info=True)
        
        logger.info(f"ETF日线数据清理完成，共清理 {cleaned_count} 个文件")
        return {"status": "success", "cleaned_count": cleaned_count}
    
    except Exception as e:
        error_msg = f"ETF数据清理失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}

def clean_log_files():
    """
    清理日志文件，只保留最近7天的日志
    """
    try:
        logger.info("开始清理日志文件...")
        
        # 获取当前日期
        current_date = get_beijing_time().date()
        
        # 计算保留日志的起始日期（7天前）
        cutoff_date = current_date - timedelta(days=7)
        logger.info(f"日志保留策略：只保留 {cutoff_date} 之后的日志")
        
        # 遍历日志目录
        cleaned_count = 0
        for filename in os.listdir(Config.LOG_DIR):
            if not filename.endswith('.log'):
                continue
                
            file_path = os.path.join(Config.LOG_DIR, filename)
            
            try:
                # 获取文件最后修改时间
                file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                
                # 如果文件修改时间早于截止日期，删除文件
                if file_mtime < cutoff_date:
                    os.remove(file_path)
                    logger.info(f"日志文件 {filename} 已删除（修改时间: {file_mtime}）")
                    cleaned_count += 1
                
            except Exception as e:
                logger.error(f"清理日志文件 {filename} 失败: {str(e)}", exc_info=True)
        
        logger.info(f"日志文件清理完成，共清理 {cleaned_count} 个文件")
        return {"status": "success", "cleaned_count": cleaned_count}
    
    except Exception as e:
        error_msg = f"日志清理失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}

def clean_temp_files():
    """
    清理临时文件和错误记录，只保留最近7天的数据
    """
    try:
        logger.info("开始清理临时文件和错误记录...")
        
        # 获取当前日期
        current_date = get_beijing_time().date()
        
        # 计算保留临时文件的起始日期（7天前）
        cutoff_date = current_date - timedelta(days=7)
        logger.info(f"临时文件保留策略：只保留 {cutoff_date} 之后的文件")
        
        # 清理标志文件目录
        cleaned_count = 0
        if os.path.exists(Config.FLAG_DIR):
            for filename in os.listdir(Config.FLAG_DIR):
                file_path = os.path.join(Config.FLAG_DIR, filename)
                
                try:
                    # 获取文件最后修改时间
                    file_mtime = datetime.datetime.fromtimestamp(os.path.getmtime(file_path)).date()
                    
                    # 如果文件修改时间早于截止日期，删除文件
                    if file_mtime < cutoff_date:
                        os.remove(file_path)
                        logger.info(f"标志文件 {filename} 已删除（修改时间: {file_mtime}）")
                        cleaned_count += 1
                    
                except Exception as e:
                    logger.error(f"清理标志文件 {filename} 失败: {str(e)}", exc_info=True)
        
        # 清理失败ETF记录
        failed_file = os.path.join(Config.ETFS_DAILY_DIR, "failed_etfs.txt")
        if os.path.exists(failed_file):
            try:
                # 读取文件内容
                with open(failed_file, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # 过滤保留日期之后的记录
                kept_lines = []
                for line in lines:
                    try:
                        parts = line.strip().split('|')
                        if len(parts) >= 3:
                            timestamp = parts[2]
                            record_date = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S").date()
                            if record_date >= cutoff_date:
                                kept_lines.append(line)
                    except Exception:
                        # 无法解析的行保留
                        kept_lines.append(line)
                
                # 保存过滤后的记录
                if len(kept_lines) < len(lines):
                    with open(failed_file, 'w', encoding='utf-8') as f:
                        f.writelines(kept_lines)
                    logger.info(f"失败ETF记录清理完成，保留 {len(kept_lines)} 条记录")
                    cleaned_count += 1
            
            except Exception as e:
                logger.error(f"清理失败ETF记录失败: {str(e)}", exc_info=True)
        
        logger.info(f"临时文件清理完成，共清理 {cleaned_count} 个项目")
        return {"status": "success", "cleaned_count": cleaned_count}
    
    except Exception as e:
        error_msg = f"临时文件清理失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {"status": "error", "message": error_msg}

def clean_all():
    """
    执行所有清理任务
    """
    try:
        results = {
            "etf_data": clean_etf_data(),
            "log_files": clean_log_files(),
            "temp_files": clean_temp_files()
        }
        
        # 检查是否有失败的任务
        if any(result["status"] == "error" for result in results.values()):
            overall_status = "partial_success"
            message = "部分清理任务失败"
        else:
            overall_status = "success"
            message = "所有清理任务完成"
        
        logger.info(f"数据清理任务完成: {message}")
        return {
            "status": overall_status,
            "message": message,
            "results": results
        }
    
    except Exception as e:
        error_msg = f"数据清理任务失败: {str(e)}"
        logger.error(error_msg, exc_info=True)
        return {
            "status": "error",
            "message": error_msg
        }

# 模块初始化
try:
    # 确保必要的目录存在
    Config.init_dirs()
    logger.info("数据清理模块初始化完成")
except Exception as e:
    logger.error(f"数据清理模块初始化失败: {str(e)}", exc_info=True)