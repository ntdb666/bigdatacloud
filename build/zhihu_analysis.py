import os
import re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyecharts.charts import Bar, Pie, WordCloud, Page
from pyecharts import options as opts
from pyecharts.globals import SymbolType

def parse_mysql_info(file_path):
    """解析 mysqlinfo.txt 获取数据库连接信息"""
    info = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
        # 第一行通常是 localhost:3306/db_name
        db_url_part = lines[0].strip()
        info['url'] = f"jdbc:mysql://{db_url_part}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        # 寻找密码行 pid:xxxx
        for line in lines:
            if 'pwd:' in line:
                info['password'] = line.split('pwd:')[1].strip()
    # 默认用户名 root
    info['user'] = 'root' 
    return info

def create_spark_session():
    """创建 SparkSession 并配置 MySQL JDBC 驱动"""
    return SparkSession.builder \
        .appName("ZhihuDataAnalysis") \
        .master("local[*]") \
        .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33") \
        .getOrCreate()

def main():
    # 1. 基础路径配置
    base_dir = r"c:\Users\Pedro\Documents\Codes\大数据云计算"
    config_path = os.path.join(base_dir, "build", "mysqlinfo.txt")
    output_html = os.path.join(base_dir, "zhihu_report.html")

    print("正在解析数据库配置...")
    db_config = parse_mysql_info(config_path)

    print("正在启动 Spark Session (初次运行可能需要下载驱动插件)...")
    spark = create_spark_session()

    try:
        # 2. 读取 MySQL 数据
        print("正在从 MySQL 读取数据...")
        df = spark.read.format("jdbc") \
            .option("url", db_config['url']) \
            .option("dbtable", "zhihu_data") \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()

        # 缓存数据提高读取效率
        df.cache()
        row_count = df.count()
        print(f"成功读取 {row_count} 条数据。")

        # 3. 数据分析与可视化准备
        
        # --- (1) 发文数量分布 (直方图) ---
        print("分析发文数量分布...")
        articles_dist = df.select("articles_count").groupBy("articles_count").count().orderBy("articles_count").collect()
        bar_articles = (
            Bar()
            .add_xaxis([str(row['articles_count']) for row in articles_dist[:20]])  # 取前20个区间防止图表过挤
            .add_yaxis("用户数量", [row['count'] for row in articles_dist[:20]])
            .set_global_opts(
                title_opts=opts.TitleOpts(title="发文数量数据分布图 (前20区间)", subtitle="基于知乎用户数据"),
                xaxis_opts=opts.AxisOpts(name="发文数"),
                yaxis_opts=opts.AxisOpts(name="人数"),
            )
        )

        # --- (2) 回答问题数量分布 (直方图) ---
        print("分析回答数量分布...")
        answer_dist = df.select("answer_count").groupBy("answer_count").count().orderBy("answer_count").collect()
        bar_answers = (
            Bar()
            .add_xaxis([str(row['answer_count']) for row in answer_dist[:20]])
            .add_yaxis("用户数量", [row['count'] for row in answer_dist[:20]])
            .set_global_opts(
                title_opts=opts.TitleOpts(title="回答问题数量数据分布图 (前20区间)"),
                xaxis_opts=opts.AxisOpts(name="回答数"),
                yaxis_opts=opts.AxisOpts(name="人数"),
            )
        )

        # --- (3) 性别分布 (饼图) ---
        print("分析性别分布...")
        # 性别: 1男, 0女, -1未知
        gender_counts = df.groupBy("gender").count().collect()
        gender_map = {1: "男", 0: "女", -1: "未知"}
        pie_gender = (
            Pie()
            .add(
                "",
                [(gender_map.get(row['gender'], "其他"), row['count']) for row in gender_counts],
                radius=["40%", "75%"],
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="性别占比分布图"),
                legend_opts=opts.LegendOpts(orient="vertical", pos_top="15%", pos_left="2%"),
            )
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
        )

        # --- (4) 姓氏词云图 ---
        print("分析姓氏分布 (生成词云)...")
        # 提取姓氏：取 name 的第一个字符，且必须是中文字符
        surname_df = df.filter(F.col("name").isNotNull()) \
            .withColumn("surname", F.substring(F.col("name"), 1, 1)) \
            .filter("surname rlike '^[\\u4e00-\\u9fa5]+$'")
            
        surname_counts = surname_df.groupBy("surname").count().orderBy(F.desc("count")).limit(100).collect()
        
        wordcloud_surnames = (
            WordCloud()
            .add(
                "",
                [(row['surname'], row['count']) for row in surname_counts],
                word_size_range=[20, 100],
                shape=SymbolType.DIAMOND,
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="姓名中姓的词云图", subtitle="展示出现频次最高的 100 个姓氏")
            )
        )

        # 4. 组装 HTML 报告
        print(f"正在生成 HTML 报告: {output_html}")
        page = Page(layout=Page.SimplePageLayout)
        page.add(bar_articles, bar_answers, pie_gender, wordcloud_surnames)
        page.render(output_html)
        
        print("\n" + "="*50)
        print("分析完成！")
        print(f"报告已生成至: {output_html}")
        print("="*50)

    except Exception as e:
        print(f"运行出错: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
