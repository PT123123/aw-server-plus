/**
 * 从内容中提取标签
 * @param {string} content 要提取标签的内容
 * @returns {string[]} 提取到的标签数组
 */
export function extractTagsFromContent(content) {
    if (!content) return [];
    const matches = content.match(/#([^\s#]+)/g) || [];
    return matches.map(tag => tag.substring(1));
}