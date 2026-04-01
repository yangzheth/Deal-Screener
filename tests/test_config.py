from __future__ import annotations

from pathlib import Path
import tempfile
import unittest

from market_intel_watch.config import load_delivery_config


class ConfigTests(unittest.TestCase):
    def test_load_delivery_config_accepts_utf8_bom(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            config_dir = Path(tmp)
            payload = '{"deliveries": [{"id": "notion", "type": "notion_database", "enabled": true, "data_source_id": "collection://demo"}]}'
            (config_dir / 'delivery.json').write_text(payload, encoding='utf-8-sig')

            deliveries = load_delivery_config(config_dir)

            self.assertEqual(1, len(deliveries))
            self.assertEqual('notion_database', deliveries[0]['type'])


if __name__ == '__main__':
    unittest.main()
