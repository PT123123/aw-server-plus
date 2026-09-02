#!/bin/bash

# 测试创建笔记
echo "测试创建笔记..."
response=$(curl -X POST -H "Content-Type: application/json" -d '{"content":"测试笔记","tags":["test"]}' http://127.0.0.1:5600/inbox/notes)
echo $response
note_id=$(echo $response | jq -r '.id')

# 测试查询笔记
echo "\n测试查询笔记..."
curl -X GET http://127.0.0.1:5600/inbox/notes/$note_id

# 测试删除笔记
echo "\n测试删除笔记..."
curl -X DELETE http://127.0.0.1:5600/inbox/notes/$note_id

# 验证删除是否成功
echo "\n验证删除结果..."
curl -X GET http://127.0.0.1:5600/inbox/notes/$note_id