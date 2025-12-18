"""
Deep Search Agent主类
整合所有模块，实现完整的深度搜索流程
"""

import json
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any, List
from loguru import logger
from .llms import LLMClient
from .nodes import (
    ReportStructureNode,
    FirstSearchNode, 
    ReflectionNode,
    FirstSummaryNode,
    ReflectionSummaryNode,
    ReportFormattingNode
)
from .state import State
from .tools import BochaMultimodalSearch, BochaResponse
from .utils import settings, Settings, format_search_results_for_prompt


class DeepSearchAgent:
    """Deep Search Agent主类"""
    
    def __init__(self, config: Optional[Settings] = None):
        """
        初始化Deep Search Agent
        
        Args:
            config: 配置对象，如果不提供则自动加载
        """
        self.config = config or settings
        
        # 初始化LLM客户端
        self.llm_client = self._initialize_llm()
        
        # 初始化搜索工具集
        self.search_agency = BochaMultimodalSearch(api_key=(self.config.BOCHA_API_KEY or self.config.BOCHA_WEB_SEARCH_API_KEY))
        
        # 初始化节点
        self._initialize_nodes()
        
        # 状态
        self.state = State()
        
        # 确保输出目录存在
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)
        
        logger.info(f"Meida Agent已初始化")
        logger.info(f"使用LLM: {self.llm_client.get_model_info()}")
        logger.info(f"搜索工具集: BochaMultimodalSearch (支持5种多模态搜索工具)")
    
    def _initialize_llm(self) -> LLMClient:
        """初始化LLM客户端"""
        return LLMClient(
            api_key=(self.config.MEDIA_ENGINE_API_KEY or self.config.MINDSPIDER_API_KEY),
            model_name=(self.config.MEDIA_ENGINE_MODEL_NAME or self.config.MINDSPIDER_MODEL_NAME),
            base_url=(self.config.MEDIA_ENGINE_BASE_URL or self.config.MINDSPIDER_BASE_URL),
        )
    
    def _initialize_nodes(self):
        """初始化处理节点"""
        self.first_search_node = FirstSearchNode(self.llm_client)
        self.reflection_node = ReflectionNode(self.llm_client)
        self.first_summary_node = FirstSummaryNode(self.llm_client)
        self.reflection_summary_node = ReflectionSummaryNode(self.llm_client)
        self.report_formatting_node = ReportFormattingNode(self.llm_client)
    
    def _validate_date_format(self, date_str: str) -> bool:
        """
        验证日期格式是否为YYYY-MM-DD
        
        Args:
            date_str: 日期字符串
            
        Returns:
            是否为有效格式
        """
        if not date_str:
            return False
        
        # 检查格式
        pattern = r'^\d{4}-\d{2}-\d{2}$'
        if not re.match(pattern, date_str):
            return False
        
        # 检查日期是否有效
        try:
            datetime.strptime(date_str, '%Y-%m-%d')
            return True
        except ValueError:
            return False
    
    def execute_search_tool(self, tool_name: str, query: str, **kwargs) -> BochaResponse:
        """
        执行指定的搜索工具
        
        Args:
            tool_name: 工具名称，可选值：
                - "comprehensive_search": 全面综合搜索（默认）
                - "web_search_only": 纯网页搜索
                - "search_for_structured_data": 结构化数据查询
                - "search_last_24_hours": 24小时内最新信息
                - "search_last_week": 本周信息
            query: 搜索查询
            **kwargs: 额外参数（如max_results）
            
        Returns:
            BochaResponse对象
        """
        logger.info(f"  → 执行搜索工具: {tool_name}")
        
        if tool_name == "comprehensive_search":
            max_results = kwargs.get("max_results", 10)
            return self.search_agency.comprehensive_search(query, max_results)
        elif tool_name == "web_search_only":
            max_results = kwargs.get("max_results", 15)
            return self.search_agency.web_search_only(query, max_results)
        elif tool_name == "search_for_structured_data":
            return self.search_agency.search_for_structured_data(query)
        elif tool_name == "search_last_24_hours":
            return self.search_agency.search_last_24_hours(query)
        elif tool_name == "search_last_week":
            return self.search_agency.search_last_week(query)
        else:
            logger.info(f"  ⚠️  未知的搜索工具: {tool_name}，使用默认综合搜索")
            return self.search_agency.comprehensive_search(query)

    # 这是马欢欢修改的代码
    def analyze_multimodal(self, media_data, text_context):
        """多模态分析+文本上下文交叉验证"""
        # 1. 基础多模态分析（原逻辑，需根据实际媒体数据格式实现）
        media_analysis = self._base_media_analysis(media_data)

        # 2. 交叉验证：检查多模态结果与文本上下文是否冲突
        conflict_check = self._check_conflict(media_analysis, text_context)
        if conflict_check["has_conflict"]:
            # 若冲突，降低置信度并提示
            media_analysis["confidence"] *= 0.5
            media_analysis["conflict_note"] = f"与文本上下文冲突：{conflict_check['reason']}"
            media_analysis["final_conclusion"] = f"【需谨慎参考】{media_analysis['final_conclusion']}"

        return media_analysis

    def _base_media_analysis(self, media_data):
        """基础多模态分析（需根据实际媒体类型实现）"""
        # 示例：根据媒体数据类型（图片/视频）提取特征和关键词
        # 实际实现需调用对应的多模态模型（如CLIP、BLIP等）
        analysis = {
            "type": media_data.get("type"),  # 媒体类型：image/video
            "keywords": [],  # 从媒体中提取的关键词
            "description": "",  # 媒体内容描述
            "confidence": 1.0,  # 初始置信度
            "final_conclusion": ""  # 分析结论
        }

        # 这里添加实际的多模态分析逻辑（如调用模型接口）
        # 示例：假设从媒体数据中提取了关键词
        analysis["keywords"] = media_data.get("extracted_keywords", [])
        analysis["description"] = media_data.get("description", "未获取到描述")
        analysis["final_conclusion"] = f"媒体内容分析：{analysis['description']}"

        return analysis

    def _check_conflict(self, media_analysis, text_context):
        """检查多模态分析与文本上下文的冲突"""
        # 1. 提取双方关键词
        media_keywords = media_analysis.get("keywords", [])
        text_keywords = self._extract_keywords(text_context)

        # 2. 检测冲突（示例逻辑：关键词矛盾）
        conflicting = []
        for kw in media_keywords:
            # 若文本中存在与媒体关键词相反的表述（如媒体说"正面"，文本说"负面"）
            if f"不{kw}" in text_keywords or f"无{kw}" in text_keywords:
                conflicting.append(kw)

        return {
            "has_conflict": len(conflicting) > 0,
            "reason": f"多模态关键词{conflicting}与文本上下文矛盾"
        }

    def _extract_keywords(self, text_context):
        """从文本上下文中提取关键词（基础实现，可扩展）"""
        # 简单实现：按标点和空格分割，取有意义的词（实际可替换为NLP工具）
        if not text_context:
            return []
        # 移除标点并分割
        words = re.findall(r'\b\w+\b', text_context.lower())
        # 过滤停用词（示例）
        stopwords = {"的", "了", "是", "在", "和", "有"}
        return [w for w in words if w not in stopwords]

    def research(self, query: str, save_report: bool = True) -> str:
        """
        执行深度研究
        
        Args:
            query: 研究查询
            save_report: 是否保存报告到文件
            
        Returns:
            最终报告内容
        """
        logger.info(f"\n{'='*60}")
        logger.info(f"开始深度研究: {query}")
        logger.info(f"{'='*60}")
        
        try:
            # Step 1: 生成报告结构
            self._generate_report_structure(query)
            
            # Step 2: 处理每个段落
            self._process_paragraphs()
            
            # Step 3: 生成最终报告
            final_report = self._generate_final_report()
            
            # Step 4: 保存报告
            if save_report:
                self._save_report(final_report)
            
            logger.info(f"\n{'='*60}")
            logger.info("深度研究完成！")
            logger.info(f"{'='*60}")
            
            return final_report
            
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            logger.error(f"研究过程中发生错误: {str(e)} \n错误堆栈: {error_traceback}")
            raise e
    
    def _generate_report_structure(self, query: str):
        """生成报告结构"""
        logger.info(f"\n[步骤 1] 生成报告结构...")
        
        # 创建报告结构节点
        report_structure_node = ReportStructureNode(self.llm_client, query)
        
        # 生成结构并更新状态
        self.state = report_structure_node.mutate_state(state=self.state)
        
        _message = f"报告结构已生成，共 {len(self.state.paragraphs)} 个段落:"
        for i, paragraph in enumerate(self.state.paragraphs, 1):
            _message += f"\n  {i}. {paragraph.title}"
        logger.info(_message)
    
    def _process_paragraphs(self):
        """处理所有段落"""
        total_paragraphs = len(self.state.paragraphs)
        
        for i in range(total_paragraphs):
            logger.info(f"\n[步骤 2.{i+1}] 处理段落: {self.state.paragraphs[i].title}")
            logger.info("-" * 50)
            
            # 初始搜索和总结
            self._initial_search_and_summary(i)
            
            # 反思循环
            self._reflection_loop(i)
            
            # 标记段落完成
            self.state.paragraphs[i].research.mark_completed()
            
            progress = (i + 1) / total_paragraphs * 100
            logger.info(f"段落处理完成 ({progress:.1f}%)")
    
    def _initial_search_and_summary(self, paragraph_index: int):
        """执行初始搜索和总结"""
        paragraph = self.state.paragraphs[paragraph_index]
        
        # 准备搜索输入
        search_input = {
            "title": paragraph.title,
            "content": paragraph.content
        }
        
        # 生成搜索查询和工具选择
        logger.info("  - 生成搜索查询...")
        search_output = self.first_search_node.run(search_input)
        search_query = search_output["search_query"]
        search_tool = search_output.get("search_tool", "comprehensive_search")  # 默认工具
        reasoning = search_output["reasoning"]
        
        logger.info(f"  - 搜索查询: {search_query}")
        logger.info(f"  - 选择的工具: {search_tool}")
        logger.info(f"  - 推理: {reasoning}")
        
        # 执行搜索
        logger.info("  - 执行网络搜索...")
        
        # 处理特殊参数（新的工具集不需要日期参数处理）
        search_kwargs = {}
        if search_tool in ["comprehensive_search", "web_search_only"]:
            # 这些工具支持max_results参数
            search_kwargs["max_results"] = 10
        
        search_response = self.execute_search_tool(search_tool, search_query, **search_kwargs)
        
        # 转换为兼容格式
        search_results = []
        if search_response and search_response.webpages:
            # 每种搜索工具都有其特定的结果数量，这里取前10个作为上限
            max_results = min(len(search_response.webpages), 10)
            # 马欢欢修改了这个循环，先把原来的注释掉
            # for result in search_response.webpages[:max_results]:
            #     search_results.append({
            #         'title': result.name,
            #         'url': result.url,
            #         'content': result.snippet,
            #         'score': None,  # Bocha API不提供score
            #         'raw_content': result.snippet,
            #         'published_date': result.date_last_crawled  # 使用爬取日期
            #     })
            # 马欢欢新修改的
            for result in search_response.webpages[:max_results]:
                print("WebpageResult可用属性：", dir(result))  # 查看当前result的所有属性
                # 1. 从搜索结果中获取媒体数据（假设Bocha返回的result包含multimedia字段，存图片/视频信息）
                media_data_list = getattr(result, "multimedia", [])  # 可能有多个媒体（比如多张图片）
                text_context = getattr(result, "snippet", "")  # 该结果对应的文本上下文（用来交叉验证）

                # 2. 对每个媒体数据执行多模态分析+交叉验证
                media_analysis_list = []
                for media_data in media_data_list:
                    # 调用你之前写的analyze_multimodal函数
                    media_analysis = self.analyze_multimodal(media_data, text_context)
                    media_analysis_list.append(media_analysis)

                # 3. 把分析结果加到搜索结果里
                search_results.append({
                    'title': getattr(result, "name", ""),
                    'url': getattr(result, "url", ""),
                    'content': getattr(result, "snippet", ""),
                    'score': None,
                    'raw_content': getattr(result, "snippet", ""),
                    'published_date': getattr(result, "date_last_crawled", None),
                    'media_analysis': media_analysis_list # 新增：多模态分析结果
                })
        if search_results:
            _message = f"  - 找到 {len(search_results)} 个搜索结果" 
            for j, result in enumerate(search_results, 1):
                date_info = f" (发布于: {result.get('published_date', 'N/A')})" if result.get('published_date') else ""
                _message += f"\n    {j}. {result['title'][:50]}...{date_info}"
            logger.info(_message)
        else:
            logger.info("  - 未找到搜索结果")
        
        # 更新状态中的搜索历史
        paragraph.research.add_search_results(
            search_query,
            search_results,
            search_tool=search_tool,
            paragraph_title=paragraph.title,
        )
        
        # 生成初始总结
        logger.info("  - 生成初始总结...")
        summary_input = {
            "title": paragraph.title,
            "content": paragraph.content,
            "search_query": search_query,
            "search_results": format_search_results_for_prompt(
                search_results, self.config.SEARCH_CONTENT_MAX_LENGTH
            )
        }
        
        # 更新状态
        self.state = self.first_summary_node.mutate_state(
            summary_input, self.state, paragraph_index
        )
        
        logger.info("  - 初始总结完成")
    
    def _reflection_loop(self, paragraph_index: int):
        """执行反思循环"""
        paragraph = self.state.paragraphs[paragraph_index]
        
        for reflection_i in range(self.config.MAX_REFLECTIONS):
            logger.info(f"  - 反思 {reflection_i + 1}/{self.config.MAX_REFLECTIONS}...")
            
            # 准备反思输入
            reflection_input = {
                "title": paragraph.title,
                "content": paragraph.content,
                "paragraph_latest_state": paragraph.research.latest_summary
            }
            
            # 生成反思搜索查询
            reflection_output = self.reflection_node.run(reflection_input)
            search_query = reflection_output["search_query"]
            search_tool = reflection_output.get("search_tool", "comprehensive_search")  # 默认工具
            reasoning = reflection_output["reasoning"]
            
            logger.info(f"    反思查询: {search_query}")
            logger.info(f"    选择的工具: {search_tool}")
            logger.info(f"    反思推理: {reasoning}")
            
            # 执行反思搜索
            # 处理特殊参数
            search_kwargs = {}
            if search_tool in ["comprehensive_search", "web_search_only"]:
                # 这些工具支持max_results参数
                search_kwargs["max_results"] = 10
            
            search_response = self.execute_search_tool(search_tool, search_query, **search_kwargs)
            
            # 转换为兼容格式
            search_results = []
            if search_response and search_response.webpages:
                # 每种搜索工具都有其特定的结果数量，这里取前10个作为上限
                max_results = min(len(search_response.webpages), 10)
                # 马欢欢修改了for先把原来的注释掉
                # for result in search_response.webpages[:max_results]:
                #     search_results.append({
                #         'title': result.name,
                #         'url': result.url,
                #         'content': result.snippet,
                #         'score': None,  # Bocha API不提供score
                #         'raw_content': result.snippet,
                #         'published_date': result.date_last_crawled
                #     })
                # 马欢欢修改好的
                for result in search_response.webpages[:max_results]:
                    print("WebpageResult可用属性：", dir(result))  # 查看当前result的所有属性
                    # 1. 关键修改：用getattr获取类属性
                    media_data_list = getattr(result, "multimedia", [])
                    text_context = getattr(result, "snippet", "")

                    # 2. 多模态分析（逻辑不变）
                    media_analysis_list = []
                    for media_data in media_data_list:
                        media_analysis = self.analyze_multimodal(media_data, text_context)
                        media_analysis_list.append(media_analysis)

                    # 3. 组装结果（其他字段用getattr）
                    search_results.append({
                        'title': getattr(result, "name", ""),
                        'url': getattr(result, "url", ""),
                        'content': getattr(result, "snippet", ""),
                        'score': None,
                        'raw_content': getattr(result, "snippet", ""),
                        'published_date': getattr(result, "date_last_crawled", None),
                        'media_analysis': media_analysis_list
                    })
            if search_results:
                _message = f"    找到 {len(search_results)} 个反思搜索结果"
                for j, result in enumerate(search_results, 1):
                    date_info = f" (发布于: {result.get('published_date', 'N/A')})" if result.get('published_date') else ""
                    _message += f"\n      {j}. {result['title'][:50]}...{date_info}"
                logger.info(_message)
            else:
                logger.info("    未找到反思搜索结果")
            
            # 更新搜索历史
            paragraph.research.add_search_results(
                search_query,
                search_results,
                search_tool=search_tool,
                paragraph_title=paragraph.title,
            )
            
            # 生成反思总结
            reflection_summary_input = {
                "title": paragraph.title,
                "content": paragraph.content,
                "search_query": search_query,
                "search_results": format_search_results_for_prompt(
                    search_results, self.config.SEARCH_CONTENT_MAX_LENGTH
                ),
                "paragraph_latest_state": paragraph.research.latest_summary
            }
            
            # 更新状态
            self.state = self.reflection_summary_node.mutate_state(
                reflection_summary_input, self.state, paragraph_index
            )
            
            logger.info(f"    反思 {reflection_i + 1} 完成")
    
    def _generate_final_report(self) -> str:
        """生成最终报告"""
        logger.info(f"\n[步骤 3] 生成最终报告...")
        
        # 准备报告数据
        report_data = []
        for paragraph in self.state.paragraphs:
            report_data.append({
                "title": paragraph.title,
                "paragraph_latest_state": paragraph.research.latest_summary
            })
        
        # 格式化报告
        try:
            final_report = self.report_formatting_node.run(report_data)
        except Exception as e:
            logger.info(f"LLM格式化失败，使用备用方法: {str(e)}")
            final_report = self.report_formatting_node.format_report_manually(
                report_data, self.state.report_title
            )
        
        # 更新状态
        self.state.final_report = final_report
        self.state.mark_completed()
        
        logger.info("最终报告生成完成")
        return final_report
    
    def _save_report(self, report_content: str):
        """保存报告到文件"""
        # 生成文件名
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_safe = "".join(c for c in self.state.query if c.isalnum() or c in (' ', '-', '_')).rstrip()
        query_safe = query_safe.replace(' ', '_')[:30]
        
        filename = f"deep_search_report_{query_safe}_{timestamp}.md"
        filepath = os.path.join(self.config.OUTPUT_DIR, filename)
        
        # 保存报告
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"报告已保存到: {filepath}")
        
        # 保存状态（如果配置允许）
        if self.config.SAVE_INTERMEDIATE_STATES:
            state_filename = f"state_{query_safe}_{timestamp}.json"
            state_filepath = os.path.join(self.config.OUTPUT_DIR, state_filename)
            self.state.save_to_file(state_filepath)
            logger.info(f"状态已保存到: {state_filepath}")
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """获取进度摘要"""
        return self.state.get_progress_summary()
    
    def load_state(self, filepath: str):
        """从文件加载状态"""
        self.state = State.load_from_file(filepath)
        logger.info(f"状态已从 {filepath} 加载")
    
    def save_state(self, filepath: str):
        """保存状态到文件"""
        self.state.save_to_file(filepath)
        logger.info(f"状态已保存到 {filepath}")


def create_agent(config_file: Optional[str] = None) -> DeepSearchAgent:
    """
    创建Deep Search Agent实例的便捷函数
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        DeepSearchAgent实例
    """
    settings = Settings()
    return DeepSearchAgent(settings)
