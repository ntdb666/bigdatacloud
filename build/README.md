# 知乎数据统计分析程序 (PySpark)

这是一个基于 PySpark 的本地模式统计分析程序，用于读取 MySQL 中的知乎用户数据并生成可视化 HTML 报告。

## 环境要求
1.  **Java 8/11/17**: PySpark 运行必须安装 Java 环境。
2.  **Python 3.7+**: 建议使用 Anaconda 或虚拟环境。
3.  **MySQL 5.7**: 已根据您的数据库版本适配。

## 快速开始

### 1. 安装依赖
在终端中运行以下命令安装所需的 Python 库：
```bash
pip install -r requirements.txt
```

### 2. 配置检查
请确保 `build/mysqlinfo.txt` 文件内容正确。程序将自动读取：
- 第一行：数据库地址 (localhost:3306/cloudlocaltest)
- 带有 `pwd:` 的行：数据库密码

### 3. 运行程序
```bash
python zhihu_analysis.py
```

### 4. 查看结果
运行完成后，当前目录下会生成 `zhihu_report.html`。双击该文件即可在浏览器中查看：
- **发文数量分布**（直方图）
- **回答问题数量分布**（直方图）
- **性别占比分布**（饼图）
- **姓氏统计**（词云图）

## 注意事项
- 首次运行程序时，PySpark 会自动从 Maven 仓库下载 `mysql-connector-java` 驱动。这可能需要几分钟时间，请保持网络畅通。
- 如果您的 MySQL 用户名不是 `root`，请修改 `zhihu_analysis.py` 中的 `info['user'] = 'root'`。
