import sys
import os
import re
import json
from typing import Dict, Any, List

# --- 路径黑魔法：确保能引用到 InsightEngine ---
current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.dirname(os.path.dirname(current_dir))
if project_root not in sys.path:
    sys.path.append(project_root)

# 引用项目工具
from InsightEngine.tools.search import MediaCrawlerDB
import requests


class LinkRepairAgent:
    """
    链接修复特工 v2.0 (增强版)：修复 KeyError bug 并优化搜索匹配
    """

    def __init__(self):
        print("🔧 初始化链接修复特工...")
        try:
            self.db_tool = MediaCrawlerDB()
            print("✅ 已连接舆情数据库 (MediaCrawlerDB)")
        except Exception as e:
            print(f"⚠️ 数据库连接失败: {e}")
            self.db_tool = None

    def _is_url_alive(self, url: str) -> bool:
        """检测链接是否存活"""
        if not url or len(url) < 10 or 'http' not in url:
            return False
        # 过滤掉明显的假链接/截断链接
        if '...' in url or 'example.com' in url:
            return False

        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            r = requests.head(url, headers=headers, timeout=2)
            if r.status_code < 400: return True
            r = requests.get(url, headers=headers, timeout=3, stream=True)
            if r.status_code < 400: return True
        except:
            return False
        return False

    def _find_real_url_from_db(self, query_text: str) -> str:
        """从数据库反查真实链接"""
        if not self.db_tool or not query_text:
            return None

        # 1. 优化关键词提取：保留空格，避免把 "2025 校庆" 变成 "2025校庆" 导致搜索变差
        # 只保留汉字、字母、数字和空格
        clean_query = re.sub(r'[^\w\u4e00-\u9fa5\s]', ' ', query_text)
        # 去掉多余空格并截取前20个字符（太长搜不到）
        clean_query = " ".join(clean_query.split())[:20]

        if len(clean_query) < 2: return None

        print(f"   🔎 正在库中重搜线索: '{clean_query}'...", end="")

        try:
            # 限制返回1条
            response = self.db_tool.search_topic_globally(topic=clean_query, limit_per_table=1)
            if response.results and len(response.results) > 0:
                candidate = response.results[0]
                if candidate.url and "http" in candidate.url:
                    print(f" [✅ 找到: {candidate.url[:30]}...]")
                    return candidate.url
        except Exception as e:
            print(f" [搜索出错: {e}]")

        print(" [❌ 未找到]")
        return None

    def repair_process(self, report_data: Dict[str, Any]) -> Dict[str, Any]:
        """主流程：安全遍历并修复"""
        fixed_count = 0
        print("🚀 开始执行报告深度修复...")

        def process_blocks_recursive(blocks):
            nonlocal fixed_count
            for block in blocks:
                # 处理段落
                if block.get('type') == 'paragraph':
                    inlines = block.get('inlines', [])
                    for run in inlines:
                        marks = run.get('marks', [])
                        # 使用 while 循环以便安全删除元素（虽然这里我们主要是修改）
                        for mark in marks:
                            if mark.get('type') == 'link':
                                # --- 1. 安全获取 URL ---
                                attrs = mark.get('attrs', {})
                                original_url = attrs.get('href', '')
                                anchor_text = run.get('text', '')

                                # --- 2. 判断是否需要修复 ---
                                if not self._is_url_alive(original_url):
                                    # print(f"💀 发现死链: {original_url}")

                                    # --- 3. 尝试搜索真链接 ---
                                    real_url = self._find_real_url_from_db(anchor_text)

                                    if real_url:
                                        # --- 4. 【关键修复】安全赋值 ---
                                        # 如果 'attrs' 不存在，先创建它，防止 KeyError
                                        if 'attrs' not in mark:
                                            mark['attrs'] = {}

                                        mark['attrs']['href'] = real_url

                                        # 可选：在文本后加个标记证明修过了
                                        # run['text'] += " [链接已修复]"
                                        fixed_count += 1
                                    else:
                                        # 没救回来，为了不让用户点进去报错，指向空或移除
                                        if 'attrs' not in mark: mark['attrs'] = {}
                                        mark['attrs']['href'] = "javascript:void(0);"  # 点击无反应
                                        if "(来源无法访问)" not in run['text']:
                                            run['text'] += " (来源暂不可用)"

                # 递归子结构
                if 'items' in block:  # 列表
                    for item in block['items']: process_blocks_recursive(item)
                if 'blocks' in block:  # 引用块
                    process_blocks_recursive(block['blocks'])
                if 'rows' in block:  # 表格
                    for row in block['rows']:
                        for cell in row.get('cells', []):
                            if 'blocks' in cell: process_blocks_recursive(cell['blocks'])

        if 'chapters' in report_data:
            for chapter in report_data['chapters']:
                if 'blocks' in chapter:
                    process_blocks_recursive(chapter['blocks'])

        print(f"✨ 修复完成！成功挽救了 {fixed_count} 个链接。")
        return report_data


if __name__ == "__main__":
    # 测试代码
    input_path = "../../logs/report_baseline.json"
    if os.path.exists(input_path):
        with open(input_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        agent = LinkRepairAgent()
        fixed = agent.repair_process(data)
        print("测试完成")
    else:
        print("请在 ReportEngine 目录下运行或调整测试路径")