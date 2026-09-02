import unittest
import sys
import logging
from datetime import datetime, timezone
from flask.testing import FlaskClient

# ========== 关键路径修正（无需改服务器代码） ==========
# 将项目根目录加入系统路径（activitywatch 目录）
sys.path.append("/home/ted/activitywatch")  # ✅ 请根据实际路径修改

# 直接导入服务器代码中的 app（从实际文件路径导入）
from aw_inbox.inbox import app  # ✅ 你的服务器代码中 app 定义在 inbox.py 中
from aw_inbox.inbox import Inbox

class TestFlomo(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.logger = logging.getLogger(cls.__name__)  # 绑定测试类名
        cls.logger.setLevel(logging.INFO)  # 日志级别
        """初始化测试客户端（直接使用服务器的 app）"""
        app.config['TESTING'] = True
        cls.client: FlaskClient = app.test_client()
        # 使用测试专用 Inbox（假设 Inbox 支持测试模式，无则移除参数）
        cls.inbox = Inbox(testing=True)  # 需与服务器的 Inbox 初始化参数一致

    def setUp(self):
        """每个测试用例前清空数据（操作测试专用 Inbox）"""
        pass
        #self.inbox.delete_all_notes()

    # ========== 测试用例（示例） ==========
    def test_api_root_endpoint(self):
        """测试根路由是否返回正确内容"""
        response = self.client.get('/')
        self.logger.error(f"回复: {response.data.decode()}")
        self.assertEqual(response.status_code, 200)
        self.assertIn("Welcome to Inbox Inbox Server", response.data.decode())

    def test_api_create_note_missing_content(self):
        """测试缺少 content 时返回 400"""
        response = self.client.post('/inbox/notes', json={"tags": ["test"]})
        self.logger.debug(f"Response: {response.data.decode()}")
        self.assertEqual(response.status_code, 400)
        self.assertIn("笔记内容不能为空", response.get_data(as_text=True))

if __name__ == '__main__':
    unittest.main(verbosity=3)