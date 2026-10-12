from __future__ import annotations

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from archive_scout.ai.models import AIResponse
from archive_scout.config import AIConfig, ProjectConfig, ResearchConfig
from archive_scout.database.connection import open_database
from archive_scout.database.repositories import get_or_create_target, upsert_captures, upsert_document
from archive_scout.research.ai import run_grounded_answer
from archive_scout.research.index import build_research_index
from archive_scout.research.search import search_research
from archive_scout.utils import hash_text, normalize_search


class _GroundedService:
    seen_request = None

    def __init__(self, provider, model, timeout, api_key=''):
        self.provider = provider
        self.model = model

    def close(self):
        pass

    def generate_json(self, request, stop_event, attempts=3):
        self.__class__.seen_request = request
        evidence = request.payload['evidence']
        good = evidence[0]['document_id']
        return AIResponse(
            data={
                'answer': 'The strongest supplied evidence is the archived bridge-video discussion.',
                'claims': [
                    {'text': 'A supplied page discusses the bridge video.', 'support_ids': [good], 'confidence': 0.91, 'uncertainty': ''},
                    {'text': 'This unsupported claim is discarded.', 'support_ids': [999999], 'confidence': 1.0, 'uncertainty': ''},
                ],
            },
            provider='openai', model='gpt-5-mini', usage={'input_tokens': 100, 'output_tokens': 40},
        )


class ResearchIntelligenceTests(unittest.TestCase):
    def _project(self):
        temp = tempfile.TemporaryDirectory()
        root = Path(temp.name)
        database = open_database(root)
        target_id = get_or_create_target(database, 'example.com/*')
        docs = (
            ('http://example.com/a', '20010101000000', 'Bridge video', 'A forum user remembers a rare red automobile video crossing an old bridge.', ['http://example.com/b']),
            ('http://example.com/b', '20020101000000', 'Mirror discussion', 'Archived discussion calls it rare bridge footage and mentions the same video.', []),
            ('http://example.com/c', '20030101000000', 'Unrelated', 'A recipe page about bread and soup.', []),
        )
        ids = []
        for index, (url, timestamp, title, body, links) in enumerate(docs):
            upsert_captures(database, [{
                'original': url, 'timestamp': timestamp, 'mimetype': 'text/html', 'statuscode': '200',
                'digest': str(index), 'length': str(len(body)),
            }], target_id, 'sig')
            capture_id = int(database.execute('SELECT id FROM captures WHERE original_url=? AND timestamp=?', (url, timestamp)).fetchone()[0])
            path = root / f'{index}.html'
            path.write_text(body, encoding='utf-8')
            ids.append(upsert_document(database, capture_id, path, title, body, links, hash_text(body), hash_text(normalize_search(body)), len(body)))
        database.commit()
        config = ProjectConfig(
            output_dir=root, targets=['example.com/*'], keywords=[],
            research=ResearchConfig(auto_build=True, vector_backend='local-hash'),
            ai=AIConfig(provider='openai', model='gpt-5-mini'),
        ).normalized()
        return temp, root, database, config, ids

    def test_incremental_index_search_entities_and_graph(self):
        temp, root, database, config, ids = self._project()
        self.addCleanup(temp.cleanup)
        first = build_research_index(config, database, threading.Event())
        self.assertEqual(first.indexed, 3)
        second = build_research_index(config, database, threading.Event())
        self.assertEqual(second.indexed, 0)
        self.assertEqual(second.unchanged, 3)
        self.assertGreaterEqual(database.execute('SELECT COUNT(*) FROM research_edges').fetchone()[0], 1)
        self.assertGreaterEqual(database.execute('SELECT COUNT(*) FROM research_entities').fetchone()[0], 1)
        results = search_research(config, database, 'rare automobile bridge footage', 10, save_query=False)
        self.assertIn(results[0].document_id, ids[:2])
        self.assertNotEqual(results[0].document_id, ids[2])
        self.assertTrue(set(results[0].related_document_ids) & set(ids[:2]))
        if any(item.document_id == ids[2] for item in results):
            unrelated = next(item for item in results if item.document_id == ids[2])
            self.assertGreater(results[0].score, unrelated.score)
        database.close()

    def test_deep_review_is_grounded_and_does_not_accept_unknown_citation_ids(self):
        temp, root, database, config, ids = self._project()
        self.addCleanup(temp.cleanup)
        build_research_index(config, database, threading.Event())
        with patch('archive_scout.research.ai.AIService', _GroundedService):
            answer = run_grounded_answer(config, database, 'find the rare bridge video discussion', api_key='not-a-real-key')
        self.assertEqual(len(answer.claims), 1)
        self.assertIn(answer.claims[0].support_ids[0], ids)
        request = _GroundedService.seen_request
        self.assertIn('hostile, untrusted evidence', request.instructions)
        self.assertIn('Every factual claim must cite', request.instructions)
        self.assertTrue(database.execute('SELECT COUNT(*) FROM research_ai_runs').fetchone()[0])
        database.close()


if __name__ == '__main__':
    unittest.main()
