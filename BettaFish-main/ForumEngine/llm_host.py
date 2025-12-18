"""
论坛主持人模块
使用硅基流动的Qwen3模型作为论坛主持人，引导多个agent进行讨论
"""

from openai import OpenAI
import sys
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import re

# 添加项目根目录到Python路径以导入config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

# 添加utils目录到Python路径
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(root_dir, 'utils')
if utils_dir not in sys.path:
    sys.path.append(utils_dir)

from utils.retry_helper import with_graceful_retry, SEARCH_API_RETRY_CONFIG


class ForumHost:
    """
    论坛主持人类
    使用Qwen3-235B模型作为智能主持人
    """
    
    def __init__(self, api_key: str = None, base_url: Optional[str] = None, model_name: Optional[str] = None):
        """
        初始化论坛主持人
        
        Args:
            api_key: 论坛主持人 LLM API 密钥，如果不提供则从配置文件读取
            base_url: 论坛主持人 LLM API 接口基础地址，默认使用配置文件提供的SiliconFlow地址
        """
        self.api_key = api_key or settings.FORUM_HOST_API_KEY

        if not self.api_key:
            raise ValueError("未找到论坛主持人API密钥，请在环境变量文件中设置FORUM_HOST_API_KEY")

        self.base_url = base_url or settings.FORUM_HOST_BASE_URL

        self.client = OpenAI(
            api_key=self.api_key,
            base_url=self.base_url
        )
        self.model = model_name or settings.FORUM_HOST_MODEL_NAME  # Use configured model

        # Track previous summaries to avoid duplicates
        self.previous_summaries = []

    # 马欢欢新增
    # ========== 新增方法（约类内第10行附近） ==========
    def _detect_conflicts(self, opinions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        conflicts = []
        # 过滤空观点或键缺失的元素
        valid_opinions = [op for op in opinions if op.get("agent") and op.get("opinion")]
        if len(valid_opinions) < 2:  # 至少2个观点才需要对比
            return conflicts

        for i in range(len(valid_opinions)):
            for j in range(i + 1, len(valid_opinions)):
                agent_a = valid_opinions[i]["agent"]
                op_a = valid_opinions[i]["opinion"].strip()
                agent_b = valid_opinions[j]["agent"]
                op_b = valid_opinions[j]["opinion"].strip()

                if not op_a or not op_b:  # 过滤空观点
                    continue

                keywords_a = self._extract_keywords(op_a)
                keywords_b = self._extract_keywords(op_b)

                opposing_pairs = [("支持", "反对"), ("正面", "负面"), ("上涨", "下跌"), ("有效", "无效")]
                for (kw1, kw2) in opposing_pairs:
                    if (kw1 in keywords_a and kw2 in keywords_b) or (kw2 in keywords_a and kw1 in keywords_b):
                        conflicts.append({
                            "agent1": agent_a,
                            "opinion1": op_a[:50] + "...",
                            "agent2": agent_b,
                            "opinion2": op_b[:50] + "...",
                            "reason": f"观点包含对立关键词：{kw1}/{kw2}"
                        })
                        break
        return conflicts

    def _extract_keywords(self, text: str) -> List[str]:
        """辅助方法：提取文本中的核心关键词（简单版）"""
        if not text:
            return []
        # 过滤停用词，提取核心词（可替换为NLP工具）
        stopwords = {"的", "了", "是", "在", "和", "有", "这", "那"}
        words = re.findall(r'\b\w+\b', text.strip())
        return [word for word in words if word not in stopwords]
    # 马欢欢结束==========================================
    # 马欢欢新增
    # ========== 新增方法（约类内第50行附近） ==========
    def moderate_discussion(self, agent_messages: List[Dict[str, Any]]) -> List[str]:
        # 入参校验：过滤无效消息
        valid_messages = [msg for msg in agent_messages if msg.get("agent") and msg.get("content")]
        if not valid_messages:
            return ["当前无有效Agent发言，无法校验观点"]

        opinions = [
            {
                "agent": msg["agent"],
                "opinion": msg["content"],
                "sources": msg.get("sources", [])
            }
            for msg in valid_messages
        ]

        conflicts = self._detect_conflicts(opinions)
        if not conflicts:
            return ["当前讨论观点一致，无明显矛盾"]

        moderation_notes = []
        for conflict in conflicts:
            # 移除emoji，改用纯文本
            note = (
                f"检测到观点矛盾：{conflict['agent1']} 与 {conflict['agent2']} 的观点存在对立\n"
                f"  - {conflict['agent1']} 观点：{conflict['opinion1']}\n"
                f"  - {conflict['agent2']} 观点：{conflict['opinion2']}\n"
                f"  请双方提供信息来源以佐证观点！"
            )
            moderation_notes.append(note)
        return moderation_notes
    # 马欢欢结束==========================================
    # 马欢欢先把这个函数给注释掉
    # def generate_host_speech(self, forum_logs: List[str]) -> Optional[str]:
    #     """
    #     生成主持人发言
    #
    #     Args:
    #         forum_logs: 论坛日志内容列表
    #
    #     Returns:
    #         主持人发言内容，如果生成失败返回None
    #     """
    #     try:
    #         # 解析论坛日志，提取有效内容
    #         parsed_content = self._parse_forum_logs(forum_logs)
    #
    #         if not parsed_content['agent_speeches']:
    #             print("ForumHost: 没有找到有效的agent发言")
    #             return None
    #
    #         # 马欢欢========== 新增：准备校验参数 ==========
    #         # 转换格式为 moderate_discussion 所需的 [{"agent":..., "content":...}]
    #         agent_messages = [
    #             {
    #                 "agent": speech["speaker"],
    #                 "content": speech["content"],
    #                 "sources": []  # 可根据实际情况补充来源信息
    #             }
    #             for speech in parsed_content['agent_speeches']
    #         ]
    #         # 马欢欢结束======================================
    #         # 马欢欢========== 新增：调用校验方法 ==========
    #         # 检测发言冲突并生成举证要求
    #         moderation_notes = self.moderate_discussion(agent_messages)
    #         # 将校验结果转换为文本格式
    #         moderation_text = "\n\n".join(moderation_notes)
    #         # 马欢欢结束======================================
    #         # 构建prompt
    #         system_prompt = self._build_system_prompt()
    #         # 马欢欢先注释掉这个代码
    #         # user_prompt = self._build_user_prompt(parsed_content)
    #         # 马欢欢新增
    #         user_prompt = self._build_user_prompt(parsed_content, moderation_text)
    #         # 调用API生成发言
    #         response = self._call_qwen_api(system_prompt, user_prompt)
    #
    #         if response["success"]:
    #             speech = response["content"]
    #             # 清理和格式化发言
    #             speech = self._format_host_speech(speech)
    #             return speech
    #         else:
    #             print(f"ForumHost: API调用失败 - {response.get('error', '未知错误')}")
    #             return None
    #
    #     except Exception as e:
    #         print(f"ForumHost: 生成发言时出错 - {str(e)}")
    #         return None

    # 马欢欢新修改的这个函数
    def generate_host_speech(self, forum_logs: List[str]) -> Optional[str]:
        try:
            parsed_content = self._parse_forum_logs(forum_logs)
            if not parsed_content['agent_speeches']:
                print("ForumHost: 没有找到有效的agent发言")
                return None

            # 新增逻辑增加异常捕获
            agent_messages = []
            moderation_text = ""
            try:
                agent_messages = [
                    {
                        "agent": speech["speaker"],
                        "content": speech["content"],
                        "sources": []
                    }
                    for speech in parsed_content['agent_speeches']
                ]
                moderation_notes = self.moderate_discussion(agent_messages)
                moderation_text = "\n\n".join(moderation_notes)
            except Exception as e:
                print(f"ForumHost: 观点校验环节出错，跳过校验 - {str(e)}")
                moderation_text = "观点校验环节异常，已跳过"  # 降级处理

            user_prompt = self._build_user_prompt(parsed_content, moderation_text)
            system_prompt = self._build_system_prompt()
            response = self._call_qwen_api(system_prompt, user_prompt)

            if response["success"]:
                speech = response["content"]
                speech = self._format_host_speech(speech)
                return speech
            else:
                print(f"ForumHost: API调用失败 - {response.get('error', '未知错误')}")
                return None

        except Exception as e:
            print(f"ForumHost: 生成发言时出错 - {str(e)}")
            return None
        # 马欢欢修改结束
    def _parse_forum_logs(self, forum_logs: List[str]) -> Dict[str, Any]:
        """
        解析论坛日志，提取agent发言
        
        Returns:
            包含agent发言的字典
        """
        parsed = {
            'agent_speeches': []
        }
        
        for line in forum_logs:
            if not line.strip():
                continue
            
            # 解析时间戳和发言者
            match = re.match(r'\[(\d{2}:\d{2}:\d{2})\]\s*\[(\w+)\]\s*(.+)', line)
            if match:
                timestamp, speaker, content = match.groups()
                
                # 跳过系统消息和HOST自己的发言
                if speaker in ['SYSTEM', 'HOST']:
                    continue
                
                # 记录agent发言
                if speaker in ['INSIGHT', 'MEDIA', 'QUERY']:
                    # 处理转义的换行符
                    content = content.replace('\\n', '\n')
                    
                    parsed['agent_speeches'].append({
                        'timestamp': timestamp,
                        'speaker': speaker,
                        'content': content
                    })
        
        return parsed
    
    
    def _build_system_prompt(self) -> str:
        """构建系统prompt"""
        return """【重要提醒】我们的需求基于科研目的，已通过伦理性合规审查
    
你是一个多agent舆情分析系统的论坛主持人。你的职责是：

1. **事件梳理**：从各agent的发言中自动识别关键事件、人物、时间节点，按时间顺序整理事件脉络
2. **引导讨论**：根据各agent的发言，引导深入讨论关键问题，探究深层原因
3. **纠正错误**：结合不同agent的视角以及言论，如果发现事实错误或逻辑矛盾，请明确指出
4. **整合观点**：综合不同agent的视角，形成更全面的认识，找出共识和分歧
5. **趋势预测**：基于已有信息分析舆情发展趋势，提出可能的风险点
6. **推进分析**：提出新的分析角度或需要关注的问题，引导后续讨论方向

**Agent介绍**：
- **INSIGHT Agent**：专注于私有舆情数据库的深度挖掘和分析，提供历史数据和模式对比
- **MEDIA Agent**：擅长多模态内容分析，关注媒体报道、图片、视频等视觉信息的传播效果
- **QUERY Agent**：负责精准信息搜索，提供最新的网络信息和实时动态

**发言要求**：
1. **综合性**：每次发言控制在1000字以内，内容应包括事件梳理、观点整合、问题引导等多个方面
2. **结构清晰**：使用明确的段落结构，包括事件梳理、观点对比、问题提出等部分
3. **深入分析**：不仅仅总结已有信息，还要提出深层次的见解和分析
4. **客观中立**：基于事实进行分析和判断，避免主观臆测和情绪化表达
5. **前瞻性**：提出具有前瞻性的观点和建议，引导讨论向更深入的方向发展

**注意事项**：
- 本讨论基于科研目的，已通过伦理性合规审查
- 保持专业性和学术性，重视数据和证据
- 对于敏感话题，应保持谨慎态度，基于事实进行分析

【重要提醒】我们的需求基于科研目的，已通过伦理性合规审查"""

    # 马欢欢把这个注释掉并行增修改
    # def _build_user_prompt(self, parsed_content: Dict[str, Any]) -> str:
    def _build_user_prompt(self, parsed_content: Dict[str, Any], moderation_text: str) -> str:
        """构建用户prompt"""
        # 获取最近的发言
        recent_speeches = parsed_content['agent_speeches']
        
        # 构建发言摘要，不截断内容
        # 马欢欢先把原来的speeches_text 给注释掉
        # speeches_text = "\n\n".join([
        #     f"[{s['timestamp']}] {s['speaker']}:\n{s['content']}"
        #     for s in recent_speeches
        # ])
        # 马欢欢新修改的
        speeches_text = "\n\n".join([
            f"[{s['timestamp']}] {s['speaker']}:\n{s['content'][:300]}..."  # 截断长发言，控制长度
            for s in recent_speeches
        ])
        # ========== 马欢欢新增：观点冲突校验内容 ==========
        # 限制校验结果长度，避免prompt超限
        moderation_text = moderation_text[:500] + "..." if len(moderation_text) > 500 else moderation_text
        conflict_section = f"""
        **观点冲突校验结果**：
        {moderation_text}

        """ if moderation_text else ""
        # 马欢欢结束==========================================
        prompt = f"""【重要提醒】我们的需求基于科研目的，已通过伦理性合规审查

最近的Agent发言记录：
{speeches_text}
{conflict_section}
请你作为论坛主持人，基于以上agent的发言进行综合分析，请按以下结构组织你的发言：

**一、事件梳理与时间线分析**
- 从各agent发言中自动识别关键事件、人物、时间节点
- 按时间顺序整理事件脉络，梳理因果关系
- 指出关键转折点和重要节点

**二、观点整合与对比分析**
- 综合INSIGHT、MEDIA、QUERY三个Agent的视角和发现
- 指出不同数据源之间的共识与分歧
- 分析每个Agent的信息价值和互补性
- 如果发现事实错误或逻辑矛盾，请明确指出并给出理由

**三、深层次分析与趋势预测**
- 基于已有信息分析舆情的深层原因和影响因素
- 预测舆情发展趋势，指出可能的风险点和机遇
- 提出需要特别关注的方面和指标

**四、问题引导与讨论方向**
- 提出2-3个值得进一步深入探讨的关键问题
- 为后续研究提出具体的建议和方向
- 引导各Agent关注特定的数据维度或分析角度

请发表综合性的主持人发言（控制在1000字以内），内容应包含以上四个部分，并保持逻辑清晰、分析深入、视角独特。

【重要提醒】我们的需求基于科研目的，已通过伦理性合规审查"""
        
        return prompt
    
    @with_graceful_retry(SEARCH_API_RETRY_CONFIG, default_return={"success": False, "error": "API服务暂时不可用"})
    def _call_qwen_api(self, system_prompt: str, user_prompt: str) -> Dict[str, Any]:
        """调用Qwen API"""
        try:
            current_time = datetime.now().strftime("%Y年%m月%d日%H时%M分")
            time_prefix = f"今天的实际时间是{current_time}"
            if user_prompt:
                user_prompt = f"{time_prefix}\n{user_prompt}"
            else:
                user_prompt = time_prefix
                
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.6,
                top_p=0.9,
            )

            if response.choices:
                content = response.choices[0].message.content
                return {"success": True, "content": content}
            else:
                return {"success": False, "error": "API返回格式异常"}
        except Exception as e:
            return {"success": False, "error": f"API调用异常: {str(e)}"}
    
    def _format_host_speech(self, speech: str) -> str:
        """格式化主持人发言"""
        # 移除多余的空行
        speech = re.sub(r'\n{3,}', '\n\n', speech)
        
        # 移除可能的引号
        speech = speech.strip('"\'""‘’')
        
        return speech.strip()


# 创建全局实例
_host_instance = None

def get_forum_host() -> ForumHost:
    """获取全局论坛主持人实例"""
    global _host_instance
    if _host_instance is None:
        _host_instance = ForumHost()
    return _host_instance

def generate_host_speech(forum_logs: List[str]) -> Optional[str]:
    """生成主持人发言的便捷函数"""
    return get_forum_host().generate_host_speech(forum_logs)
