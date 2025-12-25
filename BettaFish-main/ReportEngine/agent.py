"""
Report Agent主类。

该模块串联模板选择、布局设计、章节生成、IR装订与HTML渲染等
所有子流程，是Report Engine的总调度中心。核心职责包括：
1. 管理输入数据与状态，协调三个分析引擎、论坛日志与模板；
2. 按节点顺序驱动模板选择→布局生成→篇幅规划→章节写作→装订渲染；
3. 负责错误兜底、流式事件分发、落盘清单与最终成果保存。
"""

import json
import os
from copy import deepcopy
from pathlib import Path
from uuid import uuid4
from datetime import datetime
from typing import Optional, Dict, Any, List, Callable, Tuple

from loguru import logger

from .core import (
    ChapterStorage,
    DocumentComposer,
    TemplateSection,
    parse_template_sections,
)
from .ir import IRValidator
from .llms import LLMClient
from .nodes import (
    TemplateSelectionNode,
    ChapterGenerationNode,
    ChapterJsonParseError,
    ChapterContentError,
    DocumentLayoutNode,
    WordBudgetNode,
)
from .renderers import HTMLRenderer
from .state import ReportState
from .utils.config import settings, Settings

# === 【马欢欢新增】引入链接修复特工 ===
# 脚本存为 ReportEngine/ir/link_repair.py
try:
    from .ir.link_repair import LinkRepairAgent
except ImportError:
    # 避免如果没有这个文件导致整个项目跑不起来
    LinkRepairAgent = None


class StageOutputFormatError(ValueError):
    """阶段性输出结构不符合预期时抛出的受控异常。"""


class FileCountBaseline:
    """
    文件数量基准管理器。

    该工具用于：
    - 在任务启动时记录 Insight/Media/Query 三个引擎导出的 Markdown 数量；
    - 在后续轮询中快速判断是否有新报告落地；
    - 为 Flask 层提供“输入是否准备完毕”的依据。
    """
    
    def __init__(self):
        """
        初始化时优先尝试读取既有的基准快照。

        若 `logs/report_baseline.json` 不存在则会自动创建一份空快照，
        以便后续 `initialize_baseline` 在首次运行时写入真实基准。
        """
        self.baseline_file = 'logs/report_baseline.json'
        self.baseline_data = self._load_baseline()
    
    def _load_baseline(self) -> Dict[str, int]:
        """
        加载基准数据。

        - 当快照文件存在时直接解析JSON；
        - 捕获所有加载异常并返回空字典，保证调用方逻辑简洁。
        """
        try:
            if os.path.exists(self.baseline_file):
                with open(self.baseline_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.exception(f"加载基准数据失败: {e}")
        return {}
    
    def _save_baseline(self):
        """
        将当前基准写入磁盘。

        采用 `ensure_ascii=False` + 缩进格式，方便人工查看；
        若目标目录缺失则自动创建。
        """
        try:
            os.makedirs(os.path.dirname(self.baseline_file), exist_ok=True)
            with open(self.baseline_file, 'w', encoding='utf-8') as f:
                json.dump(self.baseline_data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.exception(f"保存基准数据失败: {e}")
    
    def initialize_baseline(self, directories: Dict[str, str]) -> Dict[str, int]:
        """
        初始化文件数量基准。

        遍历每个引擎目录并统计 `.md` 文件数量，将结果持久化为
        初始基准。后续 `check_new_files` 会据此对比增量。
        """
        current_counts = {}
        
        for engine, directory in directories.items():
            if os.path.exists(directory):
                md_files = [f for f in os.listdir(directory) if f.endswith('.md')]
                current_counts[engine] = len(md_files)
            else:
                current_counts[engine] = 0
        
        # 保存基准数据
        self.baseline_data = current_counts.copy()
        self._save_baseline()
        
        logger.info(f"文件数量基准已初始化: {current_counts}")
        return current_counts
    
    def check_new_files(self, directories: Dict[str, str]) -> Dict[str, Any]:
        """
        检查是否有新文件。

        对比当前目录文件数与基准：
        - 统计新增数量，并判定是否所有引擎都已准备就绪；
        - 返回详细计数、缺失列表，供 Web 层提示给用户。
        """
        current_counts = {}
        new_files_found = {}
        all_have_new = True
        
        for engine, directory in directories.items():
            if os.path.exists(directory):
                md_files = [f for f in os.listdir(directory) if f.endswith('.md')]
                current_counts[engine] = len(md_files)
                baseline_count = self.baseline_data.get(engine, 0)
                
                if current_counts[engine] > baseline_count:
                    new_files_found[engine] = current_counts[engine] - baseline_count
                else:
                    new_files_found[engine] = 0
                    all_have_new = False
            else:
                current_counts[engine] = 0
                new_files_found[engine] = 0
                all_have_new = False
        
        return {
            'ready': all_have_new,
            'baseline_counts': self.baseline_data,
            'current_counts': current_counts,
            'new_files_found': new_files_found,
            'missing_engines': [engine for engine, count in new_files_found.items() if count == 0]
        }
    
    def get_latest_files(self, directories: Dict[str, str]) -> Dict[str, str]:
        """
        获取每个目录的最新文件。

        通过 `os.path.getmtime` 找出最近写入的 Markdown，
        以确保生成流程永远使用最新一版三引擎报告。
        """
        latest_files = {}
        
        for engine, directory in directories.items():
            if os.path.exists(directory):
                md_files = [f for f in os.listdir(directory) if f.endswith('.md')]
                if md_files:
                    latest_file = max(md_files, key=lambda x: os.path.getmtime(os.path.join(directory, x)))
                    latest_files[engine] = os.path.join(directory, latest_file)
        
        return latest_files


class ReportAgent:
    """
    Report Agent主类。

    负责集成：
    - LLM客户端及其上层四个推理节点；
    - 章节存储、IR装订、渲染器等产出链路；
    - 状态管理、日志、输入输出校验与持久化。
    """
    _CONTENT_SPARSE_MIN_ATTEMPTS = 3
    _CONTENT_SPARSE_WARNING_TEXT = "本章LLM生成的内容字数可能过低，必要时可以尝试重新运行程序。"
    _STRUCTURAL_RETRY_ATTEMPTS = 2
    
    def __init__(self, config: Optional[Settings] = None):
        """
        初始化Report Agent。
        
        Args:
            config: 配置对象，如果不提供则自动加载
        
        步骤概览：
            1. 解析配置并接入日志/LLM/渲染等核心组件；
            2. 构造四个推理节点（模板、布局、篇幅、章节）；
            3. 初始化文件基准与章节落盘目录；
            4. 构建可序列化的状态容器，供外部服务查询。
        """
        # 加载配置
        self.config = config or settings
        
        # 初始化文件基准管理器
        self.file_baseline = FileCountBaseline()
        
        # 初始化日志
        self._setup_logging()
        
        # 初始化LLM客户端
        self.llm_client = self._initialize_llm()
        self.json_rescue_clients = self._initialize_rescue_llms()
        
        # 初始化章级存储/校验/渲染组件
        self.chapter_storage = ChapterStorage(self.config.CHAPTER_OUTPUT_DIR)
        self.document_composer = DocumentComposer()
        self.validator = IRValidator()
        self.renderer = HTMLRenderer()
        
        # 初始化节点
        self._initialize_nodes()
        
        # 初始化文件数量基准
        self._initialize_file_baseline()
        
        # 状态
        self.state = ReportState()
        
        # 确保输出目录存在
        os.makedirs(self.config.OUTPUT_DIR, exist_ok=True)
        os.makedirs(self.config.DOCUMENT_IR_OUTPUT_DIR, exist_ok=True)
        
        logger.info("Report Agent已初始化")
        logger.info(f"使用LLM: {self.llm_client.get_model_info()}")
        
    def _setup_logging(self):
        """
        设置日志。

        - 确保日志目录存在；
        - 使用独立的 loguru sink 写入 Report Engine 专属 log 文件，
          避免与其他子系统混淆。
        - 【修复】配置实时日志写入，禁用缓冲，确保前端实时看到日志
        - 【修复】防止重复添加handler
        """
        # 确保日志目录存在
        log_dir = os.path.dirname(self.config.LOG_FILE)
        os.makedirs(log_dir, exist_ok=True)

        # 【修复】检查是否已经添加过这个文件的handler，避免重复
        # loguru会自动去重，但显式检查更安全
        log_file_path = str(Path(self.config.LOG_FILE).resolve())

        # 检查现有的handlers
        handler_exists = False
        for handler_id, handler_config in logger._core.handlers.items():
            if hasattr(handler_config, 'sink'):
                sink = handler_config.sink
                # 检查是否是文件sink且路径相同
                if hasattr(sink, '_name') and sink._name == log_file_path:
                    handler_exists = True
                    logger.debug(f"日志handler已存在，跳过添加: {log_file_path}")
                    break

        if not handler_exists:
            # 【修复】创建专用的logger，配置实时写入
            # - enqueue=False: 禁用异步队列，立即写入
            # - buffering=1: 行缓冲，每条日志立即刷新到文件
            # - level="DEBUG": 记录所有级别的日志
            # - encoding="utf-8": 明确指定UTF-8编码
            # - mode="a": 追加模式，保留历史日志
            handler_id = logger.add(
                self.config.LOG_FILE,
                level="DEBUG",
                enqueue=False,      # 禁用异步队列，同步写入
                buffering=1,        # 行缓冲，每行立即写入
                serialize=False,    # 普通文本格式，不序列化为JSON
                encoding="utf-8",   # 明确UTF-8编码
                mode="a"            # 追加模式
            )
            logger.debug(f"已添加日志handler (ID: {handler_id}): {self.config.LOG_FILE}")

        # 【修复】验证日志文件可写
        try:
            with open(self.config.LOG_FILE, 'a', encoding='utf-8') as f:
                f.write('')  # 尝试写入空字符串验证权限
                f.flush()    # 立即刷新
        except Exception as e:
            logger.error(f"日志文件无法写入: {self.config.LOG_FILE}, 错误: {e}")
            raise
        
    def _initialize_file_baseline(self):
        """
        初始化文件数量基准。

        将 Insight/Media/Query 三个目录传入 `FileCountBaseline`，
        生成一次性的参考值，之后按增量判断三引擎是否产出新报告。
        """
        directories = {
            'insight': 'insight_engine_streamlit_reports',
            'media': 'media_engine_streamlit_reports',
            'query': 'query_engine_streamlit_reports'
        }
        self.file_baseline.initialize_baseline(directories)
    
    def _initialize_llm(self) -> LLMClient:
        """
        初始化LLM客户端。

        利用配置中的 API Key / 模型 / Base URL 构建统一的
        `LLMClient` 实例，为所有节点提供复用的推理入口。
        """
        return LLMClient(
            api_key=self.config.REPORT_ENGINE_API_KEY,
            model_name=self.config.REPORT_ENGINE_MODEL_NAME,
            base_url=self.config.REPORT_ENGINE_BASE_URL,
        )

    def _initialize_rescue_llms(self) -> List[Tuple[str, LLMClient]]:
        """
        初始化跨引擎章节修复所需的LLM客户端列表。

        顺序遵循“Report → Forum → Insight → Media”，缺失配置会被自动跳过。
        """
        clients: List[Tuple[str, LLMClient]] = []
        if self.llm_client:
            clients.append(("report_engine", self.llm_client))
        fallback_specs = [
            (
                "forum_engine",
                self.config.FORUM_HOST_API_KEY,
                self.config.FORUM_HOST_MODEL_NAME,
                self.config.FORUM_HOST_BASE_URL,
            ),
            (
                "insight_engine",
                self.config.INSIGHT_ENGINE_API_KEY,
                self.config.INSIGHT_ENGINE_MODEL_NAME,
                self.config.INSIGHT_ENGINE_BASE_URL,
            ),
            (
                "media_engine",
                self.config.MEDIA_ENGINE_API_KEY,
                self.config.MEDIA_ENGINE_MODEL_NAME,
                self.config.MEDIA_ENGINE_BASE_URL,
            ),
        ]
        for label, api_key, model_name, base_url in fallback_specs:
            if not api_key or not model_name:
                continue
            try:
                client = LLMClient(api_key=api_key, model_name=model_name, base_url=base_url)
            except Exception as exc:
                logger.warning(f"{label} LLM初始化失败，跳过该修复通道: {exc}")
                continue
            clients.append((label, client))
        return clients
    
    def _initialize_nodes(self):
        """
        初始化处理节点。

        顺序实例化模板选择、文档布局、篇幅规划、章节生成四个节点，
        其中章节节点额外依赖 IR 校验器与章节存储器。
        """
        self.template_selection_node = TemplateSelectionNode(
            self.llm_client,
            self.config.TEMPLATE_DIR
        )
        self.document_layout_node = DocumentLayoutNode(self.llm_client)
        self.word_budget_node = WordBudgetNode(self.llm_client)
        self.chapter_generation_node = ChapterGenerationNode(
            self.llm_client,
            self.validator,
            self.chapter_storage,
            fallback_llm_clients=self.json_rescue_clients,
            error_log_dir=self.config.JSON_ERROR_LOG_DIR,
        )
    
    def generate_report(self, query: str, reports: List[Any], forum_logs: str = "",
                        custom_template: str = "", save_report: bool = True,
                        stream_handler: Optional[Callable[[str, Dict[str, Any]], None]] = None) -> str:
        """
        生成综合报告（章节JSON → IR → HTML）。

        主要阶段：
            1. 归一化三引擎报告 + 论坛日志，并输出流式事件；
            2. 模板选择 → 模板切片 → 文档布局 → 篇幅规划；
            3. 结合篇幅目标逐章调用LLM，遇到解析错误会自动重试；
            4. 将章节装订成Document IR，再交给HTML渲染器生成成品；
            5. 可选地将HTML/IR/状态落盘，并向外界回传路径信息。

        参数:
            query: 最终要生成的报告主题或提问语句。
            reports: 来自 Query/Media/Insight 等分析引擎的原始输出，允许传入字符串或更复杂的对象。
            forum_logs: 论坛/协同记录，供LLM理解多人讨论上下文。
            custom_template: 用户指定的Markdown模板，如为空则交由模板节点自动挑选。
            save_report: 是否在生成后自动将HTML、IR与状态写入磁盘。
            stream_handler: 可选的流式事件回调，接收阶段标签与payload，用于UI实时展示。

        返回:
            dict: 包含 `html_content` 以及HTML/IR/状态文件路径的字典；若 `save_report=False` 则仅返回HTML字符串。

        异常:
            Exception: 任一子节点或渲染阶段失败时抛出，外层调用方负责兜底。
        """
        start_time = datetime.now()
        report_id = f"report-{uuid4().hex[:8]}"
        self.state.task_id = report_id
        self.state.query = query
        self.state.metadata.query = query
        self.state.mark_processing()

        normalized_reports = self._normalize_reports(reports)

        def emit(event_type: str, payload: Dict[str, Any]):
            """面向Report Engine流通道的事件分发器，保证错误不外泄。"""
            if not stream_handler:
                return
            try:
                stream_handler(event_type, payload)
            except Exception as callback_error:  # pragma: no cover - 仅记录
                logger.warning(f"流式事件回调失败: {callback_error}")

        logger.info(f"开始生成报告 {report_id}: {query}")
        logger.info(f"输入数据 - 报告数量: {len(reports)}, 论坛日志长度: {len(str(forum_logs))}")
        emit('stage', {'stage': 'agent_start', 'report_id': report_id, 'query': query})

        try:
            template_result = self._select_template(query, reports, forum_logs, custom_template)
            template_result = self._ensure_mapping(
                template_result,
                "模板选择结果",
                expected_keys=["template_name", "template_content"],
            )
            self.state.metadata.template_used = template_result.get('template_name', '')
            emit('stage', {
                'stage': 'template_selected',
                'template': template_result.get('template_name'),
                'reason': template_result.get('selection_reason')
            })
            emit('progress', {'progress': 10, 'message': '模板选择完成'})
            sections = self._slice_template(template_result.get('template_content', ''))
            if not sections:
                raise ValueError("模板无法解析出章节，请检查模板内容。")
            emit('stage', {'stage': 'template_sliced', 'section_count': len(sections)})

            template_text = template_result.get('template_content', '')
            template_overview = self._build_template_overview(template_text, sections)
            # 基于模板骨架+三引擎内容设计全局标题、目录与视觉主题
            layout_design = self._run_stage_with_retry(
                "文档设计",
                lambda: self.document_layout_node.run(
                    sections,
                    template_text,
                    normalized_reports,
                    forum_logs,
                    query,
                    template_overview,
                ),
                expected_keys=["title", "hero", "toc", "tocPlan"],
            )
            emit('stage', {
                'stage': 'layout_designed',
                'title': layout_design.get('title'),
                'toc': layout_design.get('tocTitle')
            })
            emit('progress', {'progress': 15, 'message': '文档标题/目录设计完成'})
            # 使用刚生成的设计稿对全书进行篇幅规划，约束各章字数与重点
            word_plan = self._run_stage_with_retry(
                "章节篇幅规划",
                lambda: self.word_budget_node.run(
                    sections,
                    layout_design,
                    normalized_reports,
                    forum_logs,
                    query,
                    template_overview,
                ),
                expected_keys=["chapters", "totalWords", "globalGuidelines"],
                postprocess=self._normalize_word_plan,
            )
            emit('stage', {
                'stage': 'word_plan_ready',
                'chapter_targets': len(word_plan.get('chapters', []))
            })
            emit('progress', {'progress': 20, 'message': '章节字数规划已生成'})
            # 记录每个章节的目标字数/强调点，后续传给章节LLM
            chapter_targets = {
                entry.get("chapterId"): entry
                for entry in word_plan.get("chapters", [])
                if entry.get("chapterId")
            }

            generation_context = self._build_generation_context(
                query,
                normalized_reports,
                forum_logs,
                template_result,
                layout_design,
                chapter_targets,
                word_plan,
                template_overview,
            )
            # IR/渲染需要的全局元数据，带上设计稿给出的标题/主题/目录/篇幅信息
            manifest_meta = {
                "query": query,
                "title": layout_design.get("title") or (f"{query} - 舆情洞察报告" if query else template_result.get("template_name")),
                "subtitle": layout_design.get("subtitle"),
                "tagline": layout_design.get("tagline"),
                "templateName": template_result.get("template_name"),
                "selectionReason": template_result.get("selection_reason"),
                "themeTokens": generation_context.get("theme_tokens", {}),
                "toc": {
                    "depth": 3,
                    "autoNumbering": True,
                    "title": layout_design.get("tocTitle") or "目录",
                },
                "hero": layout_design.get("hero"),
                "layoutNotes": layout_design.get("layoutNotes"),
                "wordPlan": {
                    "totalWords": word_plan.get("totalWords"),
                    "globalGuidelines": word_plan.get("globalGuidelines"),
                },
                "templateOverview": template_overview,
            }
            if layout_design.get("themeTokens"):
                manifest_meta["themeTokens"] = layout_design["themeTokens"]
            if layout_design.get("tocPlan"):
                manifest_meta["toc"]["customEntries"] = layout_design["tocPlan"]
            # 初始化章节输出目录并写入manifest，方便流式存盘
            run_dir = self.chapter_storage.start_session(report_id, manifest_meta)
            self._persist_planning_artifacts(run_dir, layout_design, word_plan, template_overview)
            emit('stage', {'stage': 'storage_ready', 'run_dir': str(run_dir)})

            chapters = []
            chapter_max_attempts = max(
                self._CONTENT_SPARSE_MIN_ATTEMPTS, self.config.CHAPTER_JSON_MAX_ATTEMPTS
            )
            total_chapters = len(sections)  # 总章节数
            completed_chapters = 0  # 已完成章节数

            for section in sections:
                logger.info(f"生成章节: {section.title}")
                emit('chapter_status', {
                    'chapterId': section.chapter_id,
                    'title': section.title,
                    'status': 'running'
                })
                # 章节流式回调：把LLM返回的delta透传给SSE，便于前端实时渲染
                def chunk_callback(delta: str, meta: Dict[str, Any], section_ref: TemplateSection = section):
                    """
                    章节内容流式回调。

                    Args:
                        delta: LLM最新输出的增量文本。
                        meta: 节点回传的章节元数据，兜底时使用。
                        section_ref: 默认指向当前章节，保证在缺失元信息时也能定位。
                    """
                    emit('chapter_chunk', {
                        'chapterId': meta.get('chapterId') or section_ref.chapter_id,
                        'title': meta.get('title') or section_ref.title,
                        'delta': delta
                    })

                chapter_payload: Dict[str, Any] | None = None
                attempt = 1
                best_sparse_candidate: Dict[str, Any] | None = None
                best_sparse_score = -1
                fallback_used = False
                while attempt <= chapter_max_attempts:
                    try:
                        chapter_payload = self.chapter_generation_node.run(
                            section,
                            generation_context,
                            run_dir,
                            stream_callback=chunk_callback
                        )
                        break
                    except (ChapterJsonParseError, ChapterContentError) as structured_error:
                        error_kind = (
                            "content_sparse" if isinstance(structured_error, ChapterContentError) else "json_parse"
                        )
                        readable_label = "内容密度异常" if error_kind == "content_sparse" else "JSON解析失败"
                        if isinstance(structured_error, ChapterContentError):
                            candidate = getattr(structured_error, "chapter_payload", None)
                            candidate_score = getattr(structured_error, "body_characters", 0) or 0
                            if isinstance(candidate, dict) and candidate_score >= 0:
                                if candidate_score > best_sparse_score:
                                    best_sparse_candidate = deepcopy(candidate)
                                    best_sparse_score = candidate_score
                        will_fallback = (
                            isinstance(structured_error, ChapterContentError)
                            and attempt >= chapter_max_attempts
                            and attempt >= self._CONTENT_SPARSE_MIN_ATTEMPTS
                            and best_sparse_candidate is not None
                        )
                        logger.warning(
                            "章节 {title} {label}（第 {attempt}/{total} 次尝试）: {error}",
                            title=section.title,
                            label=readable_label,
                            attempt=attempt,
                            total=chapter_max_attempts,
                            error=structured_error,
                        )
                        status_value = 'retrying' if attempt < chapter_max_attempts or will_fallback else 'error'
                        status_payload = {
                            'chapterId': section.chapter_id,
                            'title': section.title,
                            'status': status_value,
                            'attempt': attempt,
                            'error': str(structured_error),
                            'reason': error_kind,
                        }
                        if will_fallback:
                            status_payload['warning'] = 'content_sparse_fallback_pending'
                        emit('chapter_status', status_payload)
                        if will_fallback:
                            logger.warning(
                                "章节 {title} 达到最大尝试次数，保留字数最多（约 {score} 字）的版本作为兜底输出",
                                title=section.title,
                                score=best_sparse_score,
                            )
                            chapter_payload = self._finalize_sparse_chapter(best_sparse_candidate)
                            fallback_used = True
                            break
                        if attempt >= chapter_max_attempts:
                            raise
                        attempt += 1
                        continue
                    except Exception as chapter_error:
                        if not self._should_retry_inappropriate_content_error(chapter_error):
                            raise
                        logger.warning(
                            "章节 {title} 触发内容安全限制（第 {attempt}/{total} 次尝试），准备重新生成: {error}",
                            title=section.title,
                            attempt=attempt,
                            total=chapter_max_attempts,
                            error=chapter_error,
                        )
                        emit('chapter_status', {
                            'chapterId': section.chapter_id,
                            'title': section.title,
                            'status': 'retrying' if attempt < chapter_max_attempts else 'error',
                            'attempt': attempt,
                            'error': str(chapter_error),
                            'reason': 'content_filter'
                        })
                        if attempt >= chapter_max_attempts:
                            raise
                        attempt += 1
                        continue
                if chapter_payload is None:
                    raise ChapterJsonParseError(
                        f"{section.title} 章节JSON在 {chapter_max_attempts} 次尝试后仍无法解析"
                    )
                chapters.append(chapter_payload)
                completed_chapters += 1  # 更新已完成章节数
                # 计算当前进度：20% + 80% * (已完成章节数 / 总章节数)，四舍五入
                chapter_progress = 20 + round(80 * completed_chapters / total_chapters)
                emit('progress', {
                    'progress': chapter_progress,
                    'message': f'章节 {completed_chapters}/{total_chapters} 已完成'
                })
                completion_status = {
                    'chapterId': section.chapter_id,
                    'title': section.title,
                    'status': 'completed',
                    'attempt': attempt,
                }
                if fallback_used:
                    completion_status['warning'] = 'content_sparse_fallback'
                    completion_status['warningMessage'] = self._CONTENT_SPARSE_WARNING_TEXT
                emit('chapter_status', completion_status)

            document_ir = self.document_composer.build_document(
                report_id,
                manifest_meta,
                chapters
            )
            emit('stage', {'stage': 'chapters_compiled', 'chapter_count': len(chapters)})
            # ========================================================
            # 【新增】启动链接自愈流程 (Link Repair Loop)
            # ========================================================
            if LinkRepairAgent:
                try:
                    logger.info("🔧 启动链接修复特工，正在检测并替换失效链接...")
                    emit('stage', {'stage': 'link_repairing', 'message': '正在验证并修复报告链接...'})

                    repair_agent = LinkRepairAgent()
                    # 直接修改 document_ir 对象
                    document_ir = repair_agent.repair_process(document_ir)

                    logger.info("✅ 链接修复完成")
                except Exception as e:
                    logger.warning(f"⚠️ 链接修复过程出现异常（不影响报告生成）: {e}")
            # ========================================================
            html_report = self.renderer.render(document_ir)
            emit('stage', {'stage': 'html_rendered', 'html_length': len(html_report)})

            self.state.html_content = html_report
            self.state.mark_completed()

            saved_files = {}
            if save_report:
                saved_files = self._save_report(html_report, document_ir, report_id)
                emit('stage', {'stage': 'report_saved', 'files': saved_files})

            generation_time = (datetime.now() - start_time).total_seconds()
            self.state.metadata.generation_time = generation_time
            logger.info(f"报告生成完成，耗时: {generation_time:.2f} 秒")
            emit('metrics', {'generation_seconds': generation_time})
            return {
                'html_content': html_report,
                'report_id': report_id,
                **saved_files
            }

        except Exception as e:
            self.state.mark_failed(str(e))
            logger.exception(f"报告生成过程中发生错误: {str(e)}")
            emit('error', {'stage': 'agent_failed', 'message': str(e)})
            raise
    
    def _select_template(self, query: str, reports: List[Any], forum_logs: str, custom_template: str):
        """
        选择报告模板。

        优先使用用户指定的模板；否则将查询、三引擎报告与论坛日志
        作为上下文交给 TemplateSelectionNode，由 LLM 返回最契合的
        模板名称、内容及理由，并自动记录在状态中。

        参数:
            query: 报告主题，用于提示词聚焦行业/事件。
            reports: 多来源报告原文，帮助LLM判断结构复杂度。
            forum_logs: 对应论坛或协作讨论的文本，用于补充背景。
            custom_template: CLI/前端传入的自定义Markdown模板，非空时直接采用。

        返回:
            dict: 包含 `template_name`、`template_content` 与 `selection_reason` 的结构化结果，供后续节点消费。
        """
        logger.info("选择报告模板...")
        
        # 如果用户提供了自定义模板，直接使用
        if custom_template:
            logger.info("使用用户自定义模板")
            return {
                'template_name': 'custom',
                'template_content': custom_template,
                'selection_reason': '用户指定的自定义模板'
            }
        
        template_input = {
            'query': query,
            'reports': reports,
            'forum_logs': forum_logs
        }
        
        try:
            template_result = self.template_selection_node.run(template_input)
            
            # 更新状态
            self.state.metadata.template_used = template_result['template_name']
            
            logger.info(f"选择模板: {template_result['template_name']}")
            logger.info(f"选择理由: {template_result['selection_reason']}")
            
            return template_result
        except Exception as e:
            logger.error(f"模板选择失败，使用默认模板: {str(e)}")
            # 直接使用备用模板
            fallback_template = {
                'template_name': '社会公共热点事件分析报告模板',
                'template_content': self._get_fallback_template_content(),
                'selection_reason': '模板选择失败，使用默认社会热点事件分析模板'
            }
            self.state.metadata.template_used = fallback_template['template_name']
            return fallback_template
    
    def _slice_template(self, template_markdown: str) -> List[TemplateSection]:
        """
        将模板切成章节列表，若为空则提供fallback。

        委托 `parse_template_sections` 将Markdown标题/编号解析为
        `TemplateSection` 列表，确保后续章节生成有稳定的章节ID。
        当模板格式异常时，会回退到内置的简单骨架避免崩溃。

        参数:
            template_markdown: 完整的模板Markdown文本。

        返回:
            list[TemplateSection]: 解析后的章节序列；如解析失败则返回单章兜底结构。
        """
        sections = parse_template_sections(template_markdown)
        if sections:
            return sections
        logger.warning("模板未解析出章节，使用默认章节骨架")
        fallback = TemplateSection(
            title="1.0 综合分析",
            slug="section-1-0",
            order=10,
            depth=1,
            raw_title="1.0 综合分析",
            number="1.0",
            chapter_id="S1",
            outline=["1.1 摘要", "1.2 数据亮点", "1.3 风险提示"],
        )
        return [fallback]

    def _build_generation_context(
        self,
        query: str,
        reports: Dict[str, str],
        forum_logs: str,
        template_result: Dict[str, Any],
        layout_design: Dict[str, Any],
        chapter_directives: Dict[str, Any],
        word_plan: Dict[str, Any],
        template_overview: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        构造章节生成所需的共享上下文。

        将模板名称、布局设计、主题配色、篇幅规划、论坛日志等
        一次性整合为 `generation_context`，后续每章调用 LLM 时
        直接复用，确保所有章节共享一致的语调和视觉约束。

        参数:
            query: 用户查询词。
            reports: 归一化后的 query/media/insight 报告映射。
            forum_logs: 三引擎讨论记录。
            template_result: 模板节点返回的模板元信息。
            layout_design: 文档布局节点产出的标题/目录/主题设计。
            chapter_directives: 字数规划节点返回的章节指令映射。
            word_plan: 篇幅规划原始结果，包含全局字数约束。
            template_overview: 模板切片提炼的章节骨架摘要。

        返回:
            dict: LLM章节生成所需的全集上下文，包含主题色、布局、约束等键。
        """
        # 优先使用设计稿定制的主题色，否则退回默认主题
        theme_tokens = (
            layout_design.get("themeTokens")
            if layout_design else None
        ) or self._default_theme_tokens()

        return {
            "query": query,
            "template_name": template_result.get("template_name"),
            "reports": reports,
            "forum_logs": self._stringify(forum_logs),
            "theme_tokens": theme_tokens,
            "style_directives": {
                "tone": "analytical",
                "audience": "executive",
                "language": "zh-CN",
            },
            "data_bundles": [],
            "max_tokens": min(self.config.MAX_CONTENT_LENGTH, 6000),
            "layout": layout_design or {},
            "template_overview": template_overview or {},
            "chapter_directives": chapter_directives or {},
            "word_plan": word_plan or {},
        }

    def _normalize_reports(self, reports: List[Any]) -> Dict[str, str]:
        """
        将不同来源的报告统一转为字符串。

        约定顺序为 Query/Media/Insight，引擎提供的对象可能是
        字典或自定义类型，因此统一走 `_stringify` 做容错。

        参数:
            reports: 任意类型的报告列表，允许缺失或顺序混乱。

        返回:
            dict: 包含 `query_engine`/`media_engine`/`insight_engine` 三个字符串字段的映射。
        """
        keys = ["query_engine", "media_engine", "insight_engine"]
        normalized: Dict[str, str] = {}
        for idx, key in enumerate(keys):
            value = reports[idx] if idx < len(reports) else ""
            normalized[key] = self._stringify(value)
        return normalized

    def _should_retry_inappropriate_content_error(self, error: Exception) -> bool:
        """
        判断LLM异常是否由内容安全/不当内容导致。

        当检测到供应商返回的错误包含特定关键词时，允许章节生成
        重新尝试，以便绕过偶发的内容审查触发。

        参数:
            error: LLM客户端抛出的异常对象。

        返回:
            bool: 若匹配到内容审查关键词则返回True，否则为False。
        """
        message = str(error) if error else ""
        if not message:
            return False
        normalized = message.lower()
        keywords = [
            "inappropriate content",
            "content violation",
            "content moderation",
            "model-studio/error-code",
        ]
        return any(keyword in normalized for keyword in keywords)

    def _run_stage_with_retry(
        self,
        stage_name: str,
        fn: Callable[[], Any],
        expected_keys: Optional[List[str]] = None,
        postprocess: Optional[Callable[[Dict[str, Any], str], Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """
        运行单个LLM阶段并在结构异常时有限次重试。

        该方法只针对结构类错误做本地修复/重试，避免整个Agent重启。
        """
        last_error: Optional[Exception] = None
        for attempt in range(1, self._STRUCTURAL_RETRY_ATTEMPTS + 1):
            try:
                raw_result = fn()
                result = self._ensure_mapping(raw_result, stage_name, expected_keys)
                if postprocess:
                    result = postprocess(result, stage_name)
                return result
            except StageOutputFormatError as exc:
                last_error = exc
                logger.warning(
                    "{stage} 输出结构异常（第 {attempt}/{total} 次），将尝试修复或重试: {error}",
                    stage=stage_name,
                    attempt=attempt,
                    total=self._STRUCTURAL_RETRY_ATTEMPTS,
                    error=exc,
                )
                if attempt >= self._STRUCTURAL_RETRY_ATTEMPTS:
                    break
        raise last_error  # type: ignore[misc]

    def _ensure_mapping(
        self,
        value: Any,
        context: str,
        expected_keys: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        确保阶段输出为dict；若返回列表则尝试提取最佳匹配元素。
        """
        if isinstance(value, dict):
            return value

        if isinstance(value, list):
            candidates = [item for item in value if isinstance(item, dict)]
            if candidates:
                best = candidates[0]
                if expected_keys:
                    candidates.sort(
                        key=lambda item: sum(1 for key in expected_keys if key in item),
                        reverse=True,
                    )
                    best = candidates[0]
                logger.warning(
                    "{context} 返回列表，已自动提取包含最多预期键的元素继续执行",
                    context=context,
                )
                return best
            raise StageOutputFormatError(f"{context} 返回列表但缺少可用的对象元素")

        if value is None:
            raise StageOutputFormatError(f"{context} 返回空结果")

        raise StageOutputFormatError(
            f"{context} 返回类型 {type(value).__name__}，期望字典"
        )

    def _normalize_word_plan(self, word_plan: Dict[str, Any], stage_name: str) -> Dict[str, Any]:
        """
        清洗篇幅规划结果，确保 chapters/globalGuidelines/totalWords 类型安全。
        """
        raw_chapters = word_plan.get("chapters", [])
        if isinstance(raw_chapters, dict):
            chapters_iterable = raw_chapters.values()
        elif isinstance(raw_chapters, list):
            chapters_iterable = raw_chapters
        else:
            chapters_iterable = []

        normalized: List[Dict[str, Any]] = []
        for idx, entry in enumerate(chapters_iterable):
            if isinstance(entry, dict):
                normalized.append(entry)
                continue
            if isinstance(entry, list):
                dict_candidate = next((item for item in entry if isinstance(item, dict)), None)
                if dict_candidate:
                    logger.warning(
                        "{stage} 第 {idx} 个章节条目为列表，已提取首个对象用于后续流程",
                        stage=stage_name,
                        idx=idx + 1,
                    )
                    normalized.append(dict_candidate)
                    continue
            logger.warning(
                "{stage} 跳过无法解析的章节条目#{idx}（类型: {type_name}）",
                stage=stage_name,
                idx=idx + 1,
                type_name=type(entry).__name__,
            )

        if not normalized:
            raise StageOutputFormatError(f"{stage_name} 缺少有效的章节规划，无法继续")

        word_plan["chapters"] = normalized

        guidelines = word_plan.get("globalGuidelines")
        if not isinstance(guidelines, list):
            if guidelines is None or guidelines == "":
                word_plan["globalGuidelines"] = []
            else:
                logger.warning(
                    "{stage} globalGuidelines 类型异常，已转换为列表封装",
                    stage=stage_name,
                )
                word_plan["globalGuidelines"] = [guidelines]

        if not isinstance(word_plan.get("totalWords"), (int, float)):
            logger.warning(
                "{stage} totalWords 类型异常，使用默认值 10000",
                stage=stage_name,
            )
            word_plan["totalWords"] = 10000

        return word_plan

    def _finalize_sparse_chapter(self, chapter: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        """
        构造内容稀疏兜底章节：复制原始payload并插入温馨提示段落。
        """
        safe_chapter = deepcopy(chapter or {})
        if not isinstance(safe_chapter, dict):
            safe_chapter = {}
        self._ensure_sparse_warning_block(safe_chapter)
        return safe_chapter

    def _ensure_sparse_warning_block(self, chapter: Dict[str, Any]) -> None:
        """
        将提示段落插在章节标题后，提醒读者该章字数偏少。
        """
        warning_block = {
            "type": "paragraph",
            "inlines": [
                {
                    "text": self._CONTENT_SPARSE_WARNING_TEXT,
                    "marks": [{"type": "italic"}],
                }
            ],
            "meta": {"role": "content-sparse-warning"},
        }
        blocks = chapter.get("blocks")
        if isinstance(blocks, list) and blocks:
            inserted = False
            for idx, block in enumerate(blocks):
                if isinstance(block, dict) and block.get("type") == "heading":
                    blocks.insert(idx + 1, warning_block)
                    inserted = True
                    break
            if not inserted:
                blocks.insert(0, warning_block)
        else:
            chapter["blocks"] = [warning_block]
        meta = chapter.get("meta")
        if isinstance(meta, dict):
            meta["contentSparseWarning"] = True
        else:
            chapter["meta"] = {"contentSparseWarning": True}

    def _stringify(self, value: Any) -> str:
        """
        安全地将对象转成字符串。

        - dict/list 统一序列化为格式化 JSON，便于提示词消费；
        - 其他类型走 `str()`，None 则返回空串，避免 None 传播。

        参数:
            value: 任意Python对象。

        返回:
            str: 适配提示词/日志的字符串表现。
        """
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        if isinstance(value, (dict, list)):
            try:
                return json.dumps(value, ensure_ascii=False, indent=2)
            except Exception:
                return str(value)
        return str(value)

    def _default_theme_tokens(self) -> Dict[str, Any]:
        """
        构造默认主题变量，供渲染器/LLM共用。

        当布局节点未返回专属配色时使用该套色板，保持报告风格统一。

        返回:
            dict: 包含颜色、字体、间距、布尔开关等渲染参数的主题字典。
        """
        return {
            "colors": {
                "bg": "#f8f9fa",
                "text": "#212529",
                "primary": "#007bff",
                "secondary": "#6c757d",
                "card": "#ffffff",
                "border": "#dee2e6",
                "accent1": "#17a2b8",
                "accent2": "#28a745",
                "accent3": "#ffc107",
                "accent4": "#dc3545",
            },
            "fonts": {
                "body": "-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, 'Noto Sans', sans-serif",
                "heading": "'Source Han Sans SC', 'PingFang SC', 'Microsoft YaHei', sans-serif",
            },
            "spacing": {"container": "1200px", "gutter": "24px"},
            "vars": {
                "header_sticky": True,
                "toc_depth": 3,
                "enable_dark_mode": True,
            },
        }

    def _build_template_overview(
        self,
        template_markdown: str,
        sections: List[TemplateSection],
    ) -> Dict[str, Any]:
        """
        提取模板标题与章节骨架，供设计/篇幅规划统一引用。

        同时记录章节ID/slug/order等辅助字段，保证多节点对齐。

        参数:
            template_markdown: 模板原文，用于解析全局标题。
            sections: `TemplateSection` 列表，作为章节骨架。

        返回:
            dict: 包含模板标题与章节元数据的概览结构。
        """
        fallback_title = sections[0].title if sections else ""
        overview = {
            "title": self._extract_template_title(template_markdown, fallback_title),
            "chapters": [],
        }
        for section in sections:
            overview["chapters"].append(
                {
                    "chapterId": section.chapter_id,
                    "title": section.title,
                    "rawTitle": section.raw_title,
                    "number": section.number,
                    "slug": section.slug,
                    "order": section.order,
                    "depth": section.depth,
                    "outline": section.outline,
                }
            )
        return overview

    @staticmethod
    def _extract_template_title(template_markdown: str, fallback: str = "") -> str:
        """
        尝试从Markdown中提取首个标题。

        优先返回首个 `#` 语法标题；如果模板首行就是正文，则回退到
        第一行非空文本或调用方提供的 fallback。

        参数:
            template_markdown: 模板原文。
            fallback: 备用标题，当文档缺少显式标题时使用。

        返回:
            str: 解析到的标题文本。
        """
        for line in template_markdown.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("#"):
                return stripped.lstrip("#").strip()
            if stripped:
                fallback = fallback or stripped
        return fallback or "智能舆情分析报告"
    
    def _get_fallback_template_content(self) -> str:
        """
        获取备用模板内容。

        当模板目录不可用或LLM选择失败时使用该 Markdown 模板，
        保证后续流程仍能给出结构化章节。
        """
        return """# 社会公共热点事件分析报告

## 执行摘要
本报告针对当前社会热点事件进行综合分析，整合了多方信息源的观点和数据。

## 事件概况
### 基本信息
- 事件性质：{event_nature}
- 发生时间：{event_time}
- 涉及范围：{event_scope}

## 舆情态势分析
### 整体趋势
{sentiment_analysis}

### 主要观点分布
{opinion_distribution}

## 媒体报道分析
### 主流媒体态度
{media_analysis}

### 报道重点
{report_focus}

## 社会影响评估
### 直接影响
{direct_impact}

### 潜在影响
{potential_impact}

## 应对建议
### 即时措施
{immediate_actions}

### 长期策略
{long_term_strategy}

## 结论与展望
{conclusion}

---
*报告类型：社会公共热点事件分析*
*生成时间：{generation_time}*
"""
    
    def _save_report(self, html_content: str, document_ir: Dict[str, Any], report_id: str) -> Dict[str, Any]:
        """
        保存HTML与IR到文件并返回路径信息。

        生成基于查询和时间戳的易读文件名，同时也把运行态的
        `ReportState` 写入 JSON，方便下游排障或断点续跑。

        参数:
            html_content: 渲染后的HTML正文。
            document_ir: Document IR结构化数据。
            report_id: 当前任务ID，用于创建独立文件名。

        返回:
            dict: 记录HTML/IR/State文件的绝对与相对路径信息。
        """
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        query_safe = "".join(
            c for c in self.state.metadata.query if c.isalnum() or c in (" ", "-", "_")
        ).rstrip()
        query_safe = query_safe.replace(" ", "_")[:30] or "report"

        html_filename = f"final_report_{query_safe}_{timestamp}.html"
        html_path = Path(self.config.OUTPUT_DIR) / html_filename
        html_path.write_text(html_content, encoding="utf-8")
        html_abs = str(html_path.resolve())
        html_rel = os.path.relpath(html_abs, os.getcwd())

        ir_path = self._save_document_ir(document_ir, query_safe, timestamp)
        ir_abs = str(ir_path.resolve())
        ir_rel = os.path.relpath(ir_abs, os.getcwd())

        state_filename = f"report_state_{query_safe}_{timestamp}.json"
        state_path = Path(self.config.OUTPUT_DIR) / state_filename
        self.state.save_to_file(str(state_path))
        state_abs = str(state_path.resolve())
        state_rel = os.path.relpath(state_abs, os.getcwd())

        logger.info(f"HTML报告已保存: {html_path}")
        logger.info(f"Document IR已保存: {ir_path}")
        logger.info(f"状态已保存到: {state_path}")
        
        return {
            'report_filename': html_filename,
            'report_filepath': html_abs,
            'report_relative_path': html_rel,
            'ir_filename': ir_path.name,
            'ir_filepath': ir_abs,
            'ir_relative_path': ir_rel,
            'state_filename': state_filename,
            'state_filepath': state_abs,
            'state_relative_path': state_rel,
        }

    def _save_document_ir(self, document_ir: Dict[str, Any], query_safe: str, timestamp: str) -> Path:
        """
        将整本IR写入独立目录。

        `Document IR` 与 HTML 解耦保存，便于调试渲染差异以及
        在不重新跑 LLM 的情况下再次渲染或导出其他格式。

        参数:
            document_ir: 整本报告的IR结构。
            query_safe: 已清洗的查询短语，用于文件命名。
            timestamp: 运行时间戳，保证文件名唯一。

        返回:
            Path: 指向保存后的IR文件路径。
        """
        filename = f"report_ir_{query_safe}_{timestamp}.json"
        ir_path = Path(self.config.DOCUMENT_IR_OUTPUT_DIR) / filename
        ir_path.write_text(
            json.dumps(document_ir, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        return ir_path
    
    def _persist_planning_artifacts(
        self,
        run_dir: Path,
        layout_design: Dict[str, Any],
        word_plan: Dict[str, Any],
        template_overview: Dict[str, Any],
    ):
        """
        将文档设计稿、篇幅规划与模板概览另存成JSON。

        这些中间件文件（document_layout/word_plan/template_overview）
        方便在调试或复盘时快速定位：标题/目录/主题是如何确定的、
        字数分配有什么要求，以便后续人工校正。

        参数:
            run_dir: 章节输出根目录。
            layout_design: 文档布局节点的原始输出。
            word_plan: 篇幅规划节点输出。
            template_overview: 模板概览JSON。
        """
        artifacts = {
            "document_layout": layout_design,
            "word_plan": word_plan,
            "template_overview": template_overview,
        }
        for name, payload in artifacts.items():
            if not payload:
                continue
            path = run_dir / f"{name}.json"
            try:
                path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            except Exception as exc:
                logger.warning(f"写入{name}失败: {exc}")
    
    def get_progress_summary(self) -> Dict[str, Any]:
        """获取进度摘要，直接返回可序列化的状态字典供API层查询。"""
        return self.state.to_dict()
    
    def load_state(self, filepath: str):
        """从文件加载状态并覆盖当前state，便于断点恢复。"""
        self.state = ReportState.load_from_file(filepath)
        logger.info(f"状态已从 {filepath} 加载")
    
    def save_state(self, filepath: str):
        """保存状态到文件，通常用于任务完成后的分析与备份。"""
        self.state.save_to_file(filepath)
        logger.info(f"状态已保存到 {filepath}")
    
    def check_input_files(self, insight_dir: str, media_dir: str, query_dir: str, forum_log_path: str) -> Dict[str, Any]:
        """
        检查输入文件是否准备就绪（基于文件数量增加）。
        
        Args:
            insight_dir: InsightEngine报告目录
            media_dir: MediaEngine报告目录
            query_dir: QueryEngine报告目录
            forum_log_path: 论坛日志文件路径
            
        Returns:
            检查结果字典，包含文件计数、缺失列表、最新文件路径等
        """
        # 检查各个报告目录的文件数量变化
        directories = {
            'insight': insight_dir,
            'media': media_dir,
            'query': query_dir
        }
        
        # 使用文件基准管理器检查新文件
        check_result = self.file_baseline.check_new_files(directories)
        
        # 检查论坛日志
        forum_ready = os.path.exists(forum_log_path)
        
        # 构建返回结果
        result = {
            'ready': check_result['ready'] and forum_ready,
            'baseline_counts': check_result['baseline_counts'],
            'current_counts': check_result['current_counts'],
            'new_files_found': check_result['new_files_found'],
            'missing_files': [],
            'files_found': [],
            'latest_files': {}
        }
        
        # 构建详细信息
        for engine, new_count in check_result['new_files_found'].items():
            current_count = check_result['current_counts'][engine]
            baseline_count = check_result['baseline_counts'].get(engine, 0)
            
            if new_count > 0:
                result['files_found'].append(f"{engine}: {current_count}个文件 (新增{new_count}个)")
            else:
                result['missing_files'].append(f"{engine}: {current_count}个文件 (基准{baseline_count}个，无新增)")
        
        # 检查论坛日志
        if forum_ready:
            result['files_found'].append(f"forum: {os.path.basename(forum_log_path)}")
        else:
            result['missing_files'].append("forum: 日志文件不存在")
        
        # 获取最新文件路径（用于实际报告生成）
        if result['ready']:
            result['latest_files'] = self.file_baseline.get_latest_files(directories)
            if forum_ready:
                result['latest_files']['forum'] = forum_log_path
        
        return result
    
    def load_input_files(self, file_paths: Dict[str, str]) -> Dict[str, Any]:
        """
        加载输入文件内容
        
        Args:
            file_paths: 文件路径字典
            
        Returns:
            加载的内容字典，包含 `reports` 列表与 `forum_logs` 字符串
        """
        content = {
            'reports': [],
            'forum_logs': ''
        }
        
        # 加载报告文件
        engines = ['query', 'media', 'insight']
        for engine in engines:
            if engine in file_paths:
                try:
                    with open(file_paths[engine], 'r', encoding='utf-8') as f:
                        report_content = f.read()
                    content['reports'].append(report_content)
                    logger.info(f"已加载 {engine} 报告: {len(report_content)} 字符")
                except Exception as e:
                    logger.exception(f"加载 {engine} 报告失败: {str(e)}")
                    content['reports'].append("")
        
        # 加载论坛日志
        if 'forum' in file_paths:
            try:
                with open(file_paths['forum'], 'r', encoding='utf-8') as f:
                    content['forum_logs'] = f.read()
                logger.info(f"已加载论坛日志: {len(content['forum_logs'])} 字符")
            except Exception as e:
                logger.exception(f"加载论坛日志失败: {str(e)}")
        
        return content


def create_agent(config_file: Optional[str] = None) -> ReportAgent:
    """
    创建Report Agent实例的便捷函数。
    
    Args:
        config_file: 配置文件路径
        
    Returns:
        ReportAgent实例

    目前以环境变量驱动 `Settings`，保留 `config_file` 参数便于未来扩展。
    """
    
    config = Settings() # 以空配置初始化，而从从环境变量初始化
    return ReportAgent(config)
