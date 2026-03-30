#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Kexus OS Zone Adapter
适配域目录下的所有配置文件路径
"""

import os
import re
import sys
from pathlib import Path


def get_zone_dir():
    """获取当前脚本所在的域目录"""
    script_dir = Path(__file__).parent.resolve()
    return script_dir


def get_base_dir():
    """获取项目根目录"""
    zone_dir = get_zone_dir()
    return zone_dir.parent.parent  # zone/{zone_name} -> zone -> base


def adapt_env_file(zone_dir, base_dir):
    """适配 .env 文件"""
    env_path = zone_dir / ".env"
    if not env_path.exists():
        print(f"⚠️  .env 不存在，跳过")
        return
    
    print(f"📝 适配 .env: {env_path}")
    
    with open(env_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 替换路径
    old_dir = str(base_dir).replace('\\', '/')
    new_dir = str(zone_dir).replace('\\', '/')
    
    # 适配 Windows 和 Unix 路径
    content = re.sub(
        r'(KEXUS_PROJECT_DIR=).+',
        f'KEXUS_PROJECT_DIR={zone_dir}',
        content
    )
    
    with open(env_path, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"   ✅ 已更新 KEXUS_PROJECT_DIR -> {zone_dir}")


def adapt_python_files(zone_dir, base_dir):
    """适配 Python 算子文件"""
    py_files = list(zone_dir.glob("op_*.py"))
    
    for py_file in py_files:
        print(f"🐍 检查算子: {py_file.name}")
        
        with open(py_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # 修复可能的绝对路径导入
        # 例如: from liquid_bo import ...
        # 保持相对导入不变
        
        # 修复硬编码的路径引用
        base_str = str(base_dir).replace('\\', '\\\\')
        zone_str = str(zone_dir).replace('\\', '\\\\')
        
        content = content.replace(base_str, zone_str)
        
        if content != original:
            with open(py_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"   ✅ 已更新路径引用")
        else:
            print(f"   ✓ 无需修改")


def adapt_html_files(zone_dir, base_dir):
    """适配 HTML 文件中的 API 路径"""
    html_files = list(zone_dir.glob("op_*.html")) + [zone_dir / "index.html"]
    
    for html_file in html_files:
        if not html_file.exists():
            continue
            
        print(f"🌐 检查 HTML: {html_file.name}")
        
        with open(html_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # 检查是否有硬编码的绝对路径需要修改
        # API 路径是相对 /api/...，不需要修改
        # 静态资源路径是相对 /vendor/...，不需要修改
        
        # 如果有硬编码的 zone 路径，进行替换
        base_str = str(base_dir).replace('\\', '/')
        zone_str = str(zone_dir).replace('\\', '/')
        
        content = content.replace(base_str, zone_str)
        
        if content != original:
            with open(html_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"   ✅ 已更新路径引用")
        else:
            print(f"   ✓ 无需修改")


def adapt_json_files(zone_dir, base_dir):
    """适配 JSON 配置文件"""
    json_files = [zone_dir / "kexus_state.json"]
    
    for json_file in json_files:
        if not json_file.exists():
            continue
            
        print(f"📄 检查 JSON: {json_file.name}")
        
        with open(json_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        original = content
        
        # 替换路径
        base_str = str(base_dir).replace('\\', '/')
        zone_str = str(zone_dir).replace('\\', '/')
        
        content = content.replace(base_str, zone_str)
        
        if content != original:
            with open(json_file, 'w', encoding='utf-8') as f:
                f.write(content)
            print(f"   ✅ 已更新路径引用")
        else:
            print(f"   ✓ 无需修改")


def create_zone_meta(zone_dir):
    """创建域元数据文件"""
    meta_path = zone_dir / ".zone_meta"
    
    zone_name = zone_dir.name
    meta_content = f"""# Zone Metadata
ZONE_ID={zone_name}
ZONE_NAME={zone_name.lstrip('_').replace('_', ' ').title()}
ZONE_ICON=🌌
CREATED=2026-03-28
ADAPTED=true
"""
    
    with open(meta_path, 'w', encoding='utf-8') as f:
        f.write(meta_content)
    
    print(f"📋 已创建域元数据: {meta_path}")


def main():
    """主函数"""
    zone_dir = get_zone_dir()
    base_dir = get_base_dir()
    
    print("=" * 60)
    print("Kexus OS Zone Adapter")
    print("=" * 60)
    print(f"Zone Directory: {zone_dir}")
    print(f"Base Directory: {base_dir}")
    print("=" * 60)
    print()
    
    # 适配各类文件
    adapt_env_file(zone_dir, base_dir)
    print()
    
    adapt_python_files(zone_dir, base_dir)
    print()
    
    adapt_html_files(zone_dir, base_dir)
    print()
    
    adapt_json_files(zone_dir, base_dir)
    print()
    
    create_zone_meta(zone_dir)
    print()
    
    print("=" * 60)
    print("✅ Zone 适配完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
