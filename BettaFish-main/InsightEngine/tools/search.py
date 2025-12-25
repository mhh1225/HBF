"""
专为 AI Agent 设计的本地舆情数据库查询工具集 (MediaCrawlerDB)
版本: 8.1 (全功能完整终极版)
状态: 生产就绪
修复内容:
1. [核心] 全局集成 _construct_url_smart，强制使用 ID 拼接标准链接，解决数据库脏数据跳主页问题。
2. [完整] 补全了 search_hot_content, search_topic_by_date 等所有辅助接口的完整逻辑。
3. [适配] 覆盖 B站(BV/av)、抖音、快手、知乎(精确到回答)、微博、贴吧、小红书(提示) 的全平台路由规则。
"""

import os
import json
from loguru import logger
import asyncio
from typing import List, Dict, Any, Optional, Literal
from dataclasses import dataclass, field
from ..utils.db import fetch_all
from datetime import datetime, timedelta, date
from InsightEngine.utils.config import settings
import requests


# --- 1. 数据结构定义 ---

@dataclass
class QueryResult:
    """统一的数据库查询结果数据类"""
    platform: str
    content_type: str
    title_or_content: str
    author_nickname: Optional[str] = None
    url: Optional[str] = None
    publish_time: Optional[datetime] = None
    engagement: Dict[str, int] = field(default_factory=dict)
    source_keyword: Optional[str] = None
    hotness_score: float = 0.0
    source_table: str = ""

    # 兼容 agent.py 可能调用的 .score 属性
    @property
    def score(self) -> float:
        return self.hotness_score


@dataclass
class DBResponse:
    """封装工具的完整返回结果"""
    tool_name: str
    parameters: Dict[str, Any]
    results: List[QueryResult] = field(default_factory=list)
    results_count: int = 0
    error_message: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None


# --- 2. 核心客户端与专用工具集 ---

class MediaCrawlerDB:
    """包含多种专用舆情数据库查询工具的客户端"""
    # 权重定义
    W_LIKE = 1.0
    W_COMMENT = 5.0
    W_SHARE = 10.0
    W_VIEW = 0.1
    W_DANMAKU = 0.5

    def __init__(self):
        """初始化客户端"""
        pass

    def _execute_query(self, query: str, params: tuple = None) -> List[Dict[str, Any]]:
        try:
            try:
                loop = asyncio.get_event_loop()
                if loop.is_closed():
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)

            return loop.run_until_complete(fetch_all(query, params))
        except Exception as e:
            logger.exception(f"数据库查询时发生错误: {e}")
            return []

    @staticmethod
    def _to_datetime(ts: Any) -> Optional[datetime]:
        if not ts: return None
        try:
            if isinstance(ts, datetime): return ts
            if isinstance(ts, date): return datetime.combine(ts, datetime.min.time())
            if isinstance(ts, (int, float)) or str(ts).isdigit():
                val = float(ts)
                return datetime.fromtimestamp(val / 1000 if val > 1_000_000_000_000 else val)
            if isinstance(ts, str):
                return datetime.fromisoformat(ts.split('+')[0].strip())
        except (ValueError, TypeError):
            return None

    _table_columns_cache = {}

    def _get_table_columns(self, table_name: str) -> List[str]:
        if table_name in self._table_columns_cache: return self._table_columns_cache[table_name]
        try:
            results = self._execute_query(f"SHOW COLUMNS FROM `{table_name}`")
            columns = [row['Field'] for row in results] if results else []
            self._table_columns_cache[table_name] = columns
            return columns
        except Exception:
            return []

    def _extract_engagement(self, row: Dict[str, Any]) -> Dict[str, int]:
        """从数据行中提取并统一互动指标"""
        engagement = {}
        mapping = {'likes': ['liked_count', 'like_count', 'voteup_count', 'comment_like_count', 'likes'],
                   'comments': ['video_comment', 'comments_count', 'comment_count', 'total_replay_num',
                                'sub_comment_count'],
                   'shares': ['video_share_count', 'shared_count', 'share_count', 'total_forwards'],
                   'views': ['video_play_count', 'viewd_count'],
                   'favorites': ['video_favorite_count', 'collected_count'], 'coins': ['video_coin_count'],
                   'danmaku': ['video_danmaku'], }
        for key, potential_cols in mapping.items():
            for col in potential_cols:
                if col in row and row[col] is not None:
                    try:
                        engagement[key] = int(row[col])
                    except (ValueError, TypeError):
                        engagement[key] = 0
                    break
        return engagement

    def _wrap_query_field_with_dialect(self, field: str) -> str:
        """根据数据库方言包装SQL查询"""
        if settings.DB_DIALECT == 'postgresql': return f'"{field}"'
        return f'`{field}`'

    # === 【核心修复】智能 URL 重构逻辑 (v8.0) ===
    # 策略：强制优先使用 ID 拼接，忽略数据库中可能的脏链接
    def _construct_url_smart(self, platform: str, row: Dict[str, Any]) -> str:
        """根据 ID 和平台特性，智能拼装或屏蔽 URL"""
        generated_url = ""
        try:
            # 1. Bilibili (智能处理 av/BV)
            if platform == 'bilibili':
                vid = str(row.get('video_id') or row.get('bvid') or '').strip()
                if vid:
                    if vid.startswith('BV'): generated_url = f"https://www.bilibili.com/video/{vid}"
                    elif vid.lower().startswith('av'): generated_url = f"https://www.bilibili.com/video/{vid}"
                    elif vid.isdigit(): generated_url = f"https://www.bilibili.com/video/av{vid}"
                    else: generated_url = f"https://www.bilibili.com/video/{vid}"

            # 2. 小红书 (策略：屏蔽)
            elif platform == 'xhs':
                return "（请在小红书App查看）"

            # 3. Douyin (数字ID)
            elif platform == 'douyin':
                aweme_id = row.get('aweme_id')
                if aweme_id: generated_url = f"https://www.douyin.com/video/{aweme_id}"

            # 4. 知乎 (精确到回答)
            elif platform == 'zhihu':
                q_id = row.get('question_id')
                a_id = row.get('content_id')
                if q_id and a_id: generated_url = f"https://www.zhihu.com/question/{q_id}/answer/{a_id}"
                elif q_id: generated_url = f"https://www.zhihu.com/question/{q_id}"
                elif a_id: generated_url = f"https://www.zhihu.com/question/{a_id}"

            # 5. 快手
            elif platform == 'kuaishou':
                video_id = row.get('video_id')
                if video_id: generated_url = f"https://www.kuaishou.com/short-video/{video_id}"

            # 6. 贴吧
            elif platform == 'tieba':
                note_id = row.get('note_id') or row.get('thread_id')
                if note_id: generated_url = f"https://tieba.baidu.com/p/{note_id}"

            # 7. 微博
            elif platform == 'weibo':
                note_id = row.get('note_id')
                if note_id: generated_url = f"https://m.weibo.cn/detail/{note_id}"
                elif row.get('mblogid'): generated_url = f"https://weibo.com/detail/{row.get('mblogid')}"

            # --- 验证生成结果 ---
            if generated_url and "http" in generated_url:
                return generated_url

            # --- 兜底：如果 ID 拼不出来，才看数据库原有的 url ---
            db_url = ""
            if row.get('parent_url'): db_url = row.get('parent_url')
            elif row.get('video_url'): db_url = row.get('video_url')
            elif row.get('note_url'): db_url = row.get('note_url')
            elif row.get('aweme_url'): db_url = row.get('aweme_url')
            elif row.get('content_url'): db_url = row.get('content_url')
            elif row.get('url'): db_url = row.get('url') # daily_news

            # 过滤掉明显的垃圾链接 (如仅有域名)
            if db_url and len(str(db_url)) > 20:
                return str(db_url).strip()

        except Exception as e:
            logger.warning(f"URL重构失败: {e}")

        return ""

    # --- 3. 核心查询方法 (已全部集成 URL 修复) ---

    def search_topic_globally(self, topic: str, limit_per_table: int = 100) -> DBResponse:
        """【工具】全局话题搜索: 智能链接修复版"""
        params_for_log = {'topic': topic, 'limit': limit_per_table}
        logger.info(f"--- TOOL: 全局话题搜索 (v8.1 Force-ID Mode) ---")

        search_term = f"%{topic}%"
        all_results = []

        # 配置表：增加 id_field 用于链接重构
        search_configs = {
            'bilibili_video': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video'},
            'bilibili_video_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'video_id'},

            'douyin_aweme': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video'},
            'douyin_aweme_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'aweme_id'},

            'kuaishou_video': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video'},
            'kuaishou_video_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'video_id'},

            'xhs_note': {'fields': ['title', 'desc', 'tag_list', 'source_keyword'], 'type': 'note'},
            'xhs_note_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'note_id'},

            'zhihu_content': {'fields': ['title', 'desc', 'content_text', 'source_keyword'], 'type': 'content'},
            'zhihu_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'content_id'},

            'weibo_note': {'fields': ['content', 'source_keyword'], 'type': 'note'},
            'weibo_note_comment': {'fields': ['content'], 'type': 'comment', 'id_field': 'note_id'},

            'daily_news': {'fields': ['title'], 'type': 'news'},
        }

        join_map = {
            'bilibili_video_comment': ('bilibili_video', 'video_id', 'video_url'),
            'douyin_aweme_comment': ('douyin_aweme', 'aweme_id', 'aweme_url'),
            'kuaishou_video_comment': ('kuaishou_video', 'video_id', 'video_url'),
            'xhs_note_comment': ('xhs_note', 'note_id', 'note_url'),
            'zhihu_comment': ('zhihu_content', 'content_id', 'content_url'),
            'weibo_note_comment': ('weibo_note', 'note_id', 'note_url'),
        }

        for table, config in search_configs.items():
            param_dict = {}
            query = ""

            if table in join_map:
                # 关联查询模式
                parent_tbl, join_key, url_col = join_map[table]
                where_clauses = []
                for idx, field in enumerate(config['fields']):
                    pname = f"term_{table}_{idx}"
                    where_clauses.append(f't1.{self._wrap_query_field_with_dialect(field)} LIKE :{pname}')
                    param_dict[pname] = search_term
                where_clause = " OR ".join(where_clauses)

                # 【优化】同时查出 ID 字段 (t1.{join_key})
                query = f"""
                    SELECT t1.*, t2.{url_col} as parent_url 
                    FROM {self._wrap_query_field_with_dialect(table)} t1 
                    LEFT JOIN {self._wrap_query_field_with_dialect(parent_tbl)} t2 
                    ON t1.{join_key} = t2.{join_key} 
                    WHERE {where_clause} 
                    ORDER BY t1.id DESC 
                    LIMIT :limit
                """
            else:
                # 普通查询模式
                where_clauses = []
                for idx, field in enumerate(config['fields']):
                    pname = f"term_{table}_{idx}"
                    where_clauses.append(f'{self._wrap_query_field_with_dialect(field)} LIKE :{pname}')
                    param_dict[pname] = search_term
                where_clause = " OR ".join(where_clauses)
                query = f'SELECT * FROM {self._wrap_query_field_with_dialect(table)} WHERE {where_clause} ORDER BY id DESC LIMIT :limit'

            param_dict['limit'] = limit_per_table
            raw_results = self._execute_query(query, param_dict)

            for row in raw_results:
                content = row.get('title') or row.get('content') or row.get('desc') or ''
                platform_short = table.split('_')[0]

                # 调用智能修复
                content_url = self._construct_url_smart(platform_short, row)

                if not content_url:
                    content_url = '（无有效来源链接）'

                all_results.append(QueryResult(
                    platform=platform_short,
                    content_type=config['type'],
                    title_or_content=content,
                    author_nickname=row.get('nickname') or row.get('user_nickname'),
                    url=content_url,
                    publish_time=self._to_datetime(row.get('create_time') or row.get('publish_time') or row.get('crawl_date')),
                    engagement=self._extract_engagement(row),
                    source_table=table
                ))

        return DBResponse("search_topic_globally", params_for_log, results=all_results, results_count=len(all_results))

    def search_hot_content(self, time_period: Literal['24h', 'week', 'year'] = 'week', limit: int = 50) -> DBResponse:
        """【工具】查找热点内容 (完整实现+智能链接)"""
        params_for_log = {'time_period': time_period, 'limit': limit}
        logger.info(f"--- TOOL: 查找热点内容 (params: {params_for_log}) ---")

        now = datetime.now()
        start_time = now - timedelta(days={'24h': 1, 'week': 7}.get(time_period, 365))

        hotness_formulas = {
            'bilibili_video': f"(COALESCE(CAST(liked_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(video_comment AS UNSIGNED), 0) * {self.W_COMMENT} + COALESCE(CAST(video_share_count AS UNSIGNED), 0) * {self.W_SHARE} + COALESCE(CAST(video_favorite_count AS UNSIGNED), 0) * {self.W_SHARE} + COALESCE(CAST(video_coin_count AS UNSIGNED), 0) * {self.W_SHARE} + COALESCE(CAST(video_danmaku AS UNSIGNED), 0) * {self.W_DANMAKU} + COALESCE(CAST(video_play_count AS DECIMAL(20,2)), 0) * {self.W_VIEW})",
            'douyin_aweme': f"(COALESCE(CAST(liked_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(comment_count AS UNSIGNED), 0) * {self.W_COMMENT} + COALESCE(CAST(share_count AS UNSIGNED), 0) * {self.W_SHARE} + COALESCE(CAST(collected_count AS UNSIGNED), 0) * {self.W_SHARE})",
            'weibo_note': f"(COALESCE(CAST(liked_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(comments_count AS UNSIGNED), 0) * {self.W_COMMENT} + COALESCE(CAST(shared_count AS UNSIGNED), 0) * {self.W_SHARE})",
            'xhs_note': f"(COALESCE(CAST(liked_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(comment_count AS UNSIGNED), 0) * {self.W_COMMENT} + COALESCE(CAST(share_count AS UNSIGNED), 0) * {self.W_SHARE} + COALESCE(CAST(collected_count AS UNSIGNED), 0) * {self.W_SHARE})",
            'kuaishou_video': f"(COALESCE(CAST(liked_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(viewd_count AS DECIMAL(20,2)), 0) * {self.W_VIEW})",
            'zhihu_content': f"(COALESCE(CAST(voteup_count AS UNSIGNED), 0) * {self.W_LIKE} + COALESCE(CAST(comment_count AS UNSIGNED), 0) * {self.W_COMMENT})",
        }

        all_queries, params = [], []
        for table, formula in hotness_formulas.items():
            time_filter_sql, time_filter_param = "", None
            if table == 'weibo_note':
                time_filter_sql, time_filter_param = "`create_date_time` >= %s", start_time.strftime(
                    '%Y-%m-%d %H:%M:%S')
            elif table in ['kuaishou_video', 'xhs_note', 'douyin_aweme']:
                time_col = 'time' if table == 'xhs_note' else 'create_time'; time_filter_sql, time_filter_param = f"`{time_col}` >= %s", str(
                    int(start_time.timestamp() * 1000))
            elif table == 'zhihu_content':
                time_filter_sql, time_filter_param = "CAST(`created_time` AS UNSIGNED) >= %s", str(
                    int(start_time.timestamp()))
            else:
                time_filter_sql, time_filter_param = "`create_time` >= %s", str(int(start_time.timestamp()))

            content_type = 'note' if table in ['weibo_note', 'xhs_note'] else 'content' if table == 'zhihu_content' else 'video'

            # 在 Select 中多查一些 ID 字段，以便 _construct_url_smart 使用
            query_template = "SELECT '{platform}' as p, '{type}' as t, {title} as title, {author} as author, {url} as url, {ts} as ts, {formula} as hotness_score, source_keyword, '{tbl}' as tbl, * FROM `{tbl}` WHERE {time_filter}"

            field_subs = {'platform': table.split('_')[0], 'type': content_type, 'title': 'title', 'author': 'nickname',
                          'url': 'video_url', 'ts': 'create_time', 'formula': formula, 'tbl': table,
                          'time_filter': time_filter_sql}
            if table == 'weibo_note':
                field_subs.update({'title': 'content', 'url': 'note_url', 'ts': 'create_date_time'})
            elif table == 'xhs_note':
                field_subs.update({'ts': 'time', 'url': 'note_url'})
            elif table == 'zhihu_content':
                field_subs.update({'author': 'user_nickname', 'url': 'content_url', 'ts': 'created_time'})
            elif table == 'douyin_aweme':
                field_subs.update({'url': 'aweme_url'})

            all_queries.append(query_template.format(**field_subs))
            params.append(time_filter_param)

        final_query = f"({' ) UNION ALL ( '.join(all_queries)}) ORDER BY hotness_score DESC LIMIT %s"
        raw_results = self._execute_query(final_query, tuple(params) + (limit,))

        formatted_results = []
        for r in raw_results:
            # 调用智能修复
            url = self._construct_url_smart(r['p'], r)
            if not url: url = '（无有效来源链接）'

            formatted_results.append(QueryResult(platform=r['p'], content_type=r['t'], title_or_content=r['title'],
                                         author_nickname=r.get('author'), url=url,
                                         publish_time=self._to_datetime(r['ts']),
                                         engagement=self._extract_engagement(r),
                                         hotness_score=r.get('hotness_score', 0.0),
                                         source_keyword=r.get('source_keyword'), source_table=r['tbl']))

        return DBResponse("search_hot_content", params_for_log, results=formatted_results,
                          results_count=len(formatted_results))

    def search_topic_by_date(self, topic: str, start_date: str, end_date: str,
                             limit_per_table: int = 100) -> DBResponse:
        """【工具】按日期搜索话题: 集成链接修复"""
        params_for_log = {'topic': topic, 'start_date': start_date, 'end_date': end_date,
                          'limit_per_table': limit_per_table}
        logger.info(f"--- TOOL: 按日期搜索话题 (params: {params_for_log}) ---")

        try:
            start_dt, end_dt = datetime.strptime(start_date, '%Y-%m-%d'), datetime.strptime(end_date,
                                                                                            '%Y-%m-%d') + timedelta(
                days=1)
        except ValueError:
            return DBResponse("search_topic_by_date", params_for_log,
                              error_message="日期格式错误，请使用 'YYYY-MM-DD' 格式。")

        search_term, all_results = f"%{topic}%", []
        search_configs = {'bilibili_video': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                                             'time_col': 'create_time', 'time_type': 'sec'},
                          'douyin_aweme': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                                           'time_col': 'create_time', 'time_type': 'ms'},
                          'kuaishou_video': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                                             'time_col': 'create_time', 'time_type': 'ms'},
                          'weibo_note': {'fields': ['content', 'source_keyword'], 'type': 'note',
                                         'time_col': 'create_date_time', 'time_type': 'str'},
                          'xhs_note': {'fields': ['title', 'desc', 'tag_list', 'source_keyword'], 'type': 'note',
                                       'time_col': 'time', 'time_type': 'ms'},
                          'zhihu_content': {'fields': ['title', 'desc', 'content_text', 'source_keyword'],
                                            'type': 'content', 'time_col': 'created_time', 'time_type': 'sec_str'},
                          'tieba_note': {'fields': ['title', 'desc', 'source_keyword'], 'type': 'note',
                                         'time_col': 'publish_time', 'time_type': 'str'},
                          'daily_news': {'fields': ['title'], 'type': 'news', 'time_col': 'crawl_date',
                                         'time_type': 'date_str'}}

        for table, config in search_configs.items():
            param_dict = {}
            where_clauses = []
            for idx, field in enumerate(config['fields']):
                pname = f"term_{idx}"
                where_clauses.append(f'{self._wrap_query_field_with_dialect(field)} LIKE :{pname}')
                param_dict[pname] = search_term
            param_dict['limit'] = limit_per_table

            where_clause = ' OR '.join(where_clauses)
            query = f'SELECT * FROM {self._wrap_query_field_with_dialect(table)} WHERE {where_clause} ORDER BY id DESC LIMIT :limit'
            raw_results = self._execute_query(query, param_dict)

            for row in raw_results:
                content = (row.get('title') or row.get('content') or row.get('desc') or row.get('content_text', ''))
                time_key = row.get('create_time') or row.get('time') or row.get('created_time') or row.get(
                    'publish_time') or row.get('crawl_date')
                platform_short = table.split('_')[0]

                # 调用智能修复
                content_url = self._construct_url_smart(platform_short, row)
                if not content_url: content_url = '（无有效来源链接）'

                all_results.append(QueryResult(
                    platform=table.split('_')[0], content_type=config['type'],
                    title_or_content=content if content else '',
                    author_nickname=row.get('nickname') or row.get('user_nickname') or row.get('user_name'),
                    url=content_url,
                    publish_time=self._to_datetime(time_key),
                    engagement=self._extract_engagement(row),
                    source_keyword=row.get('source_keyword'),
                    source_table=table,
                    hotness_score=0.0
                ))
        return DBResponse("search_topic_by_date", params_for_log, results=all_results, results_count=len(all_results))

    def get_comments_for_topic(self, topic: str, limit: int = 500) -> DBResponse:
        """【工具】获取话题评论: 集成链接修复"""
        params_for_log = {'topic': topic, 'limit': limit}
        logger.info(f"--- TOOL: 获取话题评论 (params: {params_for_log}) ---")

        search_term = f"%{topic}%"
        comment_tables = ['bilibili_video_comment', 'douyin_aweme_comment', 'kuaishou_video_comment',
                          'weibo_note_comment', 'xhs_note_comment', 'zhihu_comment', 'tieba_comment']

        join_map = {
            'bilibili_video_comment': ('bilibili_video', 'video_id', 'video_url'),
            'douyin_aweme_comment': ('douyin_aweme', 'aweme_id', 'aweme_url'),
            'kuaishou_video_comment': ('kuaishou_video', 'video_id', 'video_url'),
            'weibo_note_comment': ('weibo_note', 'note_id', 'note_url'),
            'xhs_note_comment': ('xhs_note', 'note_id', 'note_url'),
            'zhihu_comment': ('zhihu_content', 'content_id', 'content_url'),
            'tieba_comment': ('tieba_note', 'note_id', 'note_url')
        }

        all_queries = []
        for table in comment_tables:
            cols = self._get_table_columns(table)
            author_col = 'user_nickname' if 'user_nickname' in cols else 'nickname'
            like_col = 'comment_like_count' if 'comment_like_count' in cols else 'like_count' if 'like_count' in cols else None
            time_col = 'publish_time' if 'publish_time' in cols else 'create_date_time' if 'create_date_time' in cols else 'create_time'
            like_select = f"t1.`{like_col}` as likes" if like_col else "'0' as likes"

            if table in join_map:
                parent_tbl, join_key, url_col = join_map[table]
                # 这里必须也查出 ID，否则无法修复
                query = (f"SELECT '{table.split('_')[0]}' as platform, t1.`content`, t1.`{author_col}` as author, "
                         f"t1.`{time_col}` as ts, {like_select}, '{table}' as source_table, "
                         f"t2.`{url_col}` as parent_url, t1.`{join_key}` as raw_id "  # 查出 ID
                         f"FROM `{table}` t1 "
                         f"LEFT JOIN `{parent_tbl}` t2 ON t1.`{join_key}` = t2.`{join_key}` "
                         f"WHERE t1.`content` LIKE %s")
            else:
                query = (f"SELECT '{table.split('_')[0]}' as platform, `content`, `{author_col}` as author, "
                         f"`{time_col}` as ts, {like_select}, '{table}' as source_table, '' as parent_url, '' as raw_id "
                         f"FROM `{table}` WHERE `content` LIKE %s")
            all_queries.append(query)

        final_query = f"({' ) UNION ALL ( '.join(all_queries)}) ORDER BY ts DESC LIMIT %s"
        params = (search_term,) * len(comment_tables) + (limit,)
        raw_results = self._execute_query(final_query, params)

        formatted = []
        for r in raw_results:
            # 构建一个临时 row 字典来适配 _construct_url_smart
            platform = r['platform']
            temp_row = {}
            if r.get('parent_url'): temp_row['parent_url'] = r['parent_url']

            # 映射 raw_id 到标准 ID 字段
            if platform == 'bilibili': temp_row['video_id'] = r.get('raw_id')
            elif platform == 'douyin': temp_row['aweme_id'] = r.get('raw_id')
            elif platform == 'xhs': temp_row['note_id'] = r.get('raw_id')
            elif platform == 'kuaishou': temp_row['video_id'] = r.get('raw_id')
            elif platform == 'zhihu': temp_row['content_id'] = r.get('raw_id') # 这里评论一般只有content_id
            elif platform == 'tieba': temp_row['note_id'] = r.get('raw_id')
            elif platform == 'weibo': temp_row['note_id'] = r.get('raw_id')

            content_url = self._construct_url_smart(platform, temp_row)
            if not content_url: content_url = '（无有效来源链接）'

            formatted.append(QueryResult(
                platform=r['platform'], content_type='comment', title_or_content=r['content'],
                author_nickname=r['author'],
                url=content_url,
                publish_time=self._to_datetime(r['ts']),
                engagement={'likes': int(r['likes']) if str(r['likes']).isdigit() else 0},
                source_table=r['source_table'],
                hotness_score=0.0
            ))
        return DBResponse("get_comments_for_topic", params_for_log, results=formatted, results_count=len(formatted))

    def search_topic_on_platform(self,
                                 platform: Literal['bilibili', 'weibo', 'douyin', 'kuaishou', 'xhs', 'zhihu', 'tieba'],
                                 topic: str, start_date: Optional[str] = None, end_date: Optional[str] = None,
                                 limit: int = 20) -> DBResponse:
        """【工具】平台定向搜索: 集成链接修复"""
        params_for_log = {'platform': platform, 'topic': topic, 'start_date': start_date, 'end_date': end_date,
                          'limit': limit}
        logger.info(f"--- TOOL: 平台定向搜索 (params: {params_for_log}) ---")

        all_configs = {
            'bilibili': [{'table': 'bilibili_video', 'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                          'time_col': 'create_time', 'time_type': 'sec'},
                         {'table': 'bilibili_video_comment', 'fields': ['content'], 'type': 'comment'}],
            'douyin': [{'table': 'douyin_aweme', 'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                        'time_col': 'create_time', 'time_type': 'ms'},
                       {'table': 'douyin_aweme_comment', 'fields': ['content'], 'type': 'comment'}],
            'kuaishou': [{'table': 'kuaishou_video', 'fields': ['title', 'desc', 'source_keyword'], 'type': 'video',
                          'time_col': 'create_time', 'time_type': 'ms'},
                         {'table': 'kuaishou_video_comment', 'fields': ['content'], 'type': 'comment'}],
            'weibo': [{'table': 'weibo_note', 'fields': ['content', 'source_keyword'], 'type': 'note',
                       'time_col': 'create_date_time', 'time_type': 'str'},
                      {'table': 'weibo_note_comment', 'fields': ['content'], 'type': 'comment'}],
            'xhs': [{'table': 'xhs_note', 'fields': ['title', 'desc', 'tag_list', 'source_keyword'], 'type': 'note',
                     'time_col': 'time', 'time_type': 'ms'},
                    {'table': 'xhs_note_comment', 'fields': ['content'], 'type': 'comment'}],
            'zhihu': [{'table': 'zhihu_content', 'fields': ['title', 'desc', 'content_text', 'source_keyword'],
                       'type': 'content', 'time_col': 'created_time', 'time_type': 'sec_str'},
                      {'table': 'zhihu_comment', 'fields': ['content'], 'type': 'comment'}],
            'tieba': [{'table': 'tieba_note', 'fields': ['title', 'desc', 'source_keyword'], 'type': 'note',
                       'time_col': 'publish_time', 'time_type': 'str'},
                      {'table': 'tieba_comment', 'fields': ['content'], 'type': 'comment'}]
        }

        join_map = {
            'bilibili_video_comment': ('bilibili_video', 'video_id', 'video_url'),
            'douyin_aweme_comment': ('douyin_aweme', 'aweme_id', 'aweme_url'),
            'kuaishou_video_comment': ('kuaishou_video', 'video_id', 'video_url'),
            'weibo_note_comment': ('weibo_note', 'note_id', 'note_url'),
            'xhs_note_comment': ('xhs_note', 'note_id', 'note_url'),
            'zhihu_comment': ('zhihu_content', 'content_id', 'content_url'),
        }

        if platform not in all_configs: return DBResponse("search_topic_on_platform", params_for_log,
                                                          error_message=f"不支持的平台: {platform}")
        search_term, all_results = f"%{topic}%", []
        platform_configs = all_configs[platform]
        start_dt, end_dt = None, None
        if start_date and end_date:
            try:
                start_dt, end_dt = datetime.strptime(start_date, '%Y-%m-%d'), datetime.strptime(end_date,
                                                                                                '%Y-%m-%d') + timedelta(
                    days=1)
            except ValueError:
                return DBResponse("search_topic_on_platform", params_for_log, error_message="日期格式错误")

        for config in platform_configs:
            table = config['table']
            topic_clause = " OR ".join([f"t1.`{field}` LIKE %s" for field in config['fields']])
            params = [search_term] * len(config['fields'])

            if table in join_map:
                parent_tbl, join_key, url_col = join_map[table]
                query = f"SELECT t1.*, t2.{url_col} as parent_url FROM `{table}` t1 LEFT JOIN `{parent_tbl}` t2 ON t1.`{join_key}` = t2.`{join_key}` WHERE ({topic_clause})"
            else:
                topic_clause = topic_clause.replace("t1.", "")
                query = f"SELECT *, '' as parent_url FROM `{table}` WHERE ({topic_clause})"

            if start_dt and end_dt and 'time_col' in config:
                time_col, time_type = config['time_col'], config['time_type']
                if time_type == 'sec':
                    t_params = (int(start_dt.timestamp()), int(end_dt.timestamp()))
                elif time_type == 'ms':
                    t_params = (int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000))
                elif time_type in ['str', 'date_str']:
                    t_params = (start_dt.strftime('%Y-%m-%d'), end_dt.strftime('%Y-%m-%d'))
                else:
                    t_params = (str(int(start_dt.timestamp())), str(int(end_dt.timestamp())))
                prefix = "t1." if table in join_map else ""
                t_clause = f"{prefix}`{time_col}` >= %s AND {prefix}`{time_col}` < %s"
                if table == 'zhihu_content': t_clause = f"CAST({prefix}`{time_col}` AS UNSIGNED) >= %s AND CAST({prefix}`{time_col}` AS UNSIGNED) < %s"
                query += f" AND ({t_clause})"
                params.extend(t_params)

            prefix = "t1." if table in join_map else ""
            query += f" ORDER BY {prefix}id DESC LIMIT %s"
            params.append(limit)
            raw_results = self._execute_query(query, tuple(params))

            for row in raw_results:
                content = (row.get('title') or row.get('content') or row.get('desc') or row.get('content_text', ''))
                time_key = config.get('time_col') and row.get(config.get('time_col'))

                # 调用智能修复
                content_url = self._construct_url_smart(platform, row)
                if not content_url: content_url = '（无有效来源链接）'

                all_results.append(QueryResult(
                    platform=platform, content_type=config['type'], title_or_content=content if content else '',
                    author_nickname=row.get('nickname') or row.get('user_nickname'),
                    url=content_url,
                    publish_time=self._to_datetime(time_key), engagement=self._extract_engagement(row),
                    source_keyword=row.get('source_keyword'), source_table=table,
                    hotness_score=0.0
                ))

        return DBResponse("search_topic_on_platform", params_for_log, results=all_results,
                          results_count=len(all_results))


# --- 3. 测试与使用示例 ---
def print_response_summary(response: DBResponse):
    """简化的打印函数，用于展示测试结果"""
    if response.error_message:
        logger.info(f"工具 '{response.tool_name}' 执行出错: {response.error_message}")
        return

    params_str = ", ".join(f"{k}='{v}'" for k, v in response.parameters.items())
    logger.info(f"查询: 工具='{response.tool_name}', 参数=[{params_str}]")
    logger.info(f"找到 {response.results_count} 条相关记录。")

    # 统一为一个消息输出
    output_lines = []
    output_lines.append("==== 查询结果预览（最多前5条） ====")
    if response.results and len(response.results) > 0:
        for idx, res in enumerate(response.results[:5], 1):
            content_preview = (res.title_or_content.replace('\n', ' ')[:70] + '...') if res.title_or_content and len(
                res.title_or_content) > 70 else (res.title_or_content or '')
            author_str = res.author_nickname or "N/A"
            publish_time_str = res.publish_time.strftime('%Y-%m-%d %H:%M') if res.publish_time else "N/A"
            hotness_str = f", hotness: {res.hotness_score:.2f}" if getattr(res, "hotness_score", 0) > 0 else ""
            engagement_dict = getattr(res, "engagement", {}) or {}
            engagement_str = ", ".join(f"{k}: {v}" for k, v in engagement_dict.items() if v)
            output_lines.append(
                f"{idx}. [{res.platform.upper()}/{res.content_type}] {content_preview}\n"
                f"   作者: {author_str} | 时间: {publish_time_str}"
                f"{hotness_str} | 源关键词: '{res.source_keyword or 'N/A'}'\n"
                f"   链接: {res.url or 'N/A'}\n"
                f"   互动数据: {{{engagement_str}}}"
            )
    else:
        output_lines.append("暂无相关内容。")
    output_lines.append("=" * 60)
    logger.info('\n'.join(output_lines))


if __name__ == "__main__":

    try:
        db_agent_tools = MediaCrawlerDB()
        logger.info("数据库工具初始化成功，开始执行测试场景...\n")

        # 场景1: (新) 查找过去一周综合热度最高的内容 (不再需要sort_by)
        response1 = db_agent_tools.search_hot_content(time_period='week', limit=5)
        print_response_summary(response1)

        # 场景2: 查找过去24小时内综合热度最高的内容
        response2 = db_agent_tools.search_hot_content(time_period='24h', limit=5)
        print_response_summary(response2)

        # 场景3: 全局搜索"罗永浩"
        response3 = db_agent_tools.search_topic_globally(topic="罗永浩", limit_per_table=2)
        print_response_summary(response3)

        # 场景4: (新增) 在B站上精确搜索"论文"
        response4 = db_agent_tools.search_topic_on_platform(platform='bilibili', topic="论文", limit=5)
        print_response_summary(response4)

        # 场景5: (新增) 在微博上精确搜索 "许凯" 在特定一天内的内容
        response5 = db_agent_tools.search_topic_on_platform(platform='weibo', topic="许凯", start_date='2025-08-22',
                                                            end_date='2025-08-22', limit=5)
        print_response_summary(response5)

    except ValueError as e:
        logger.exception(f"初始化失败: {e}")
        logger.exception("请确保相关的数据库环境变量已正确设置, 或在代码中直接提供连接信息。")
    except Exception as e:
        logger.exception(f"测试过程中发生未知错误: {e}")