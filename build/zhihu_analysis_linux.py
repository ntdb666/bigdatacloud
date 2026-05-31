#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyecharts.charts import Bar, Pie, WordCloud, Page
from pyecharts import options as opts
from pyecharts.commons.utils import JsCode

def parse_mysql_info(file_path):
    """解析 mysqlinfo.txt 获取数据库连接信息"""
    info = {}
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"找不到配置文件: {file_path} ! 请确保文件存在。")
        
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = [l.strip() for l in f.readlines() if l.strip()]
        db_url_part = lines[0]
        # Linux生产环境下通常建议配置好SSL，但如果开发环境没有配置，保留该选项
        info['url'] = f"jdbc:mysql://{db_url_part}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true"
        for line in lines:
            if 'pwd:' in line:
                info['password'] = line.split('pwd:')[1].strip()
    info['user'] = 'root' 
    return info

def main():
    # 1. 动态获取 Linux 各级目录
    # 在 Linux 服务器中运行，通过标准 pathlib/os 动态获取
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir) 
    
    config_path = os.path.join(script_dir, "mysqlinfo.txt")
    output_html = os.path.join(base_dir, "zhihu_report.html")
    local_jar = os.path.join(base_dir, "mysql-connector-java-5.1.8-bin.jar")

    print("\n" + "="*50)
    print("=== PySpark Linux 部署模式 ===")
    print(f"脚本执行路径: {sys.argv[0]}")
    print(f"配置文件路径: {config_path}")
    print(f"预计生成报告: {output_html}")
    print("="*50 + "\n")

    try:
        db_config = parse_mysql_info(config_path)

        # 【修改】用户要求保留单机模式，通过 python 直接运行
        builder = SparkSession.builder.appName("ZhihuDataAnalysis_Linux").master("local[*]")
        
        # 配置驱动：同样支持本地jar和在线下载机制
        if os.path.exists(local_jar):
            builder = builder.config("spark.driver.extraClassPath", local_jar)
            driver_class = "com.mysql.jdbc.Driver"
        else:
            builder = builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            driver_class = "com.mysql.cj.jdbc.Driver"
            
        spark = builder.getOrCreate()
        print("Spark 引擎启动成功！")

        # 2. 读取 MySQL 数据
        print(f"正在尝试连接 MySQL 并读取 zhihu_data 表 (使用驱动: {driver_class})...")
        df = spark.read.format("jdbc") \
            .option("url", db_config['url']) \
            .option("dbtable", "zhihu_data") \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", driver_class) \
            .load()

        row_count = df.count()
        if row_count == 0:
            print("❌ 错误: 成功连上数据库，但数据表 'zhihu_data' 没有任何记录！")
            return

        df.cache()
        print(f"✅ 数据读取成功！共获取 {row_count} 条记录。")

        # 3. 数据分析与可视化准备
        
        # --- (1) 发文数量分布 ---
        articles_dist = df.select("articles_count").groupBy("articles_count").count().orderBy("articles_count").limit(20).collect()
        x_data = [str(row['articles_count']) for row in articles_dist]
        y_data = [row['count'] for row in articles_dist]

        bar_articles = (
            Bar(init_opts=opts.InitOpts(theme="wonderland", width="1000px", height="500px"))
            .add_xaxis(x_data)
            .add_yaxis("用户数量", y_data, 
                category_gap="40%",
                itemstyle_opts=opts.ItemStyleOpts(
                    color=JsCode("new echarts.graphic.LinearGradient(0, 0, 0, 1, [{offset: 0, color: '#83bff6'}, {offset: 0.5, color: '#188df0'}, {offset: 1, color: '#188df0'}])"),
                    border_radius=[5, 5, 0, 0]
                )
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="知乎用户发文数量分布", pos_left="center"),
                legend_opts=opts.LegendOpts(is_show=False)
            )
        )

        # --- (2) 回答问题数量分布 ---
        answer_dist = df.select("answer_count").groupBy("answer_count").count().orderBy("answer_count").limit(20).collect()
        x_data_ans = [str(row['answer_count']) for row in answer_dist]
        y_data_ans = [row['count'] for row in answer_dist]

        bar_answers = (
            Bar(init_opts=opts.InitOpts(theme="wonderland", width="1000px", height="500px"))
            .add_xaxis(x_data_ans)
            .add_yaxis("用户数量", y_data_ans,
                category_gap="40%",
                itemstyle_opts=opts.ItemStyleOpts(
                    color=JsCode("new echarts.graphic.LinearGradient(0, 0, 0, 1, [{offset: 0, color: '#6be6c1'}, {offset: 0.5, color: '#3fb1e3'}, {offset: 1, color: '#3fb1e3'}])"),
                    border_radius=[5, 5, 0, 0]
                )
            )
            .set_global_opts(
                title_opts=opts.TitleOpts(title="知乎用户回答数量分布", pos_left="center"),
                legend_opts=opts.LegendOpts(is_show=False)
            )
        )

        # --- (3) 性别分布 (玫瑰图) ---
        gender_counts = df.groupBy("gender").count().collect()
        gender_map = {1: "男", 0: "女", -1: "未知"}
        pie_data = [(gender_map.get(row['gender'], "其他"), row['count']) for row in gender_counts]

        pie_gender = (
            Pie(init_opts=opts.InitOpts(theme="wonderland", width="1000px", height="600px"))
            .add("", pie_data, radius=["30%", "70%"], rosetype="radius", itemstyle_opts=opts.ItemStyleOpts(border_radius=8))
            .set_global_opts(title_opts=opts.TitleOpts(title="性别占比分布", pos_left="center"))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
        )

        # --- (4) 姓氏词云图 ---
        surname_counts = df.filter(F.col("name").isNotNull()) \
            .withColumn("surname", F.substring(F.col("name"), 1, 1)) \
            .filter("surname rlike '^[\\u4e00-\\u9fa5]+$'") \
            .groupBy("surname").count().orderBy(F.desc("count")).limit(80).collect()
        
        wordcloud_surnames = (
            WordCloud(init_opts=opts.InitOpts(width="1000px", height="600px"))
            .add("", [(row['surname'], row['count']) for row in surname_counts], word_size_range=[25, 120], shape="circle")
            .set_global_opts(title_opts=opts.TitleOpts(title="高频姓氏词云图", pos_left="center"))
        )

        # 4. 组装并渲染
        print(f"正在生成可视化报告...")
        page = Page(layout=Page.SimplePageLayout)
        page.add(bar_articles, bar_answers, pie_gender, wordcloud_surnames)
        page.render(output_html)
        
        print("\n" + "="*50)
        print("🎉 分析圆满完成！")
        print(f"报告生成成功，请在服务器检查以下位置：\n👉 {output_html}")
        print("="*50)

    except Exception as e:
        print("\n" + "!"*50)
        print(f"❌ 运行过程中发生错误:\n{str(e)}")
        print("!"*50)
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()
