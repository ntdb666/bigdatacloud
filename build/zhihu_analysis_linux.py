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
        info['url'] = f"jdbc:mysql://{db_url_part}?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true&characterEncoding=utf8"
        for line in lines:
            if 'pwd:' in line:
                info['password'] = line.split('pwd:')[1].strip()
    info['user'] = 'cloudlocaltest' 
    return info

def main():
    import time
    total_start = time.time()

    # 1. 动态获取 Linux 各级目录
    script_dir = os.path.dirname(os.path.abspath(__file__))
    base_dir = os.path.dirname(script_dir)

    config_path = os.path.join(script_dir, "mysqlinfo.txt")
    output_html = os.path.join(base_dir, "zhihu_report.html")
    local_jar = os.path.join(base_dir, "mysql-connector-java-5.1.8-bin.jar")

    print("\n" + "="*60)
    print("  PySpark 知乎数据分析 — 详细执行日志")
    print("="*60)
    print(f"  脚本路径    : {sys.argv[0]}")
    print(f"  配置文件    : {config_path}")
    print(f"  输出报告    : {output_html}")
    print(f"  本地驱动JAR : {local_jar} (存在: {os.path.exists(local_jar)})")
    print("="*60 + "\n")

    try:
        # ── 步骤1: 解析配置 ──────────────────────────────────
        print("[步骤 1/8] 解析数据库配置文件...")
        step_start = time.time()
        db_config = parse_mysql_info(config_path)
        print(f"  ├─ 数据库URL : {db_config['url']}")
        print(f"  ├─ 用户名    : {db_config['user']}")
        print(f"  └─ 密码      : {'*' * len(db_config.get('password', ''))}")
        print(f"  ✓ 配置解析完成 ({time.time()-step_start:.2f}s)\n")

        # ── 步骤2: 启动Spark ─────────────────────────────────
        print("[步骤 2/8] 启动 Spark 引擎...")
        step_start = time.time()
        builder = SparkSession.builder.appName("ZhihuDataAnalysis_Linux").master("local[*]")

        if os.path.exists(local_jar):
            builder = builder.config("spark.driver.extraClassPath", local_jar)
            driver_class = "com.mysql.jdbc.Driver"
            print(f"  ├─ 驱动模式  : 本地JAR ({local_jar})")
        else:
            builder = builder.config("spark.jars.packages", "mysql:mysql-connector-java:8.0.33")
            driver_class = "com.mysql.cj.jdbc.Driver"
            print(f"  ├─ 驱动模式  : 在线下载 (mysql-connector-java:8.0.33)")

        spark = builder.getOrCreate()
        print(f"  ├─ App名称   : {spark.sparkContext.appName}")
        print(f"  ├─ Master    : {spark.sparkContext.master}")
        print(f"  └─ Spark版本 : {spark.version}")
        print(f"  ✓ Spark 引擎启动成功 ({time.time()-step_start:.2f}s)\n")

        # ── 步骤3: 连接MySQL ─────────────────────────────────
        print("[步骤 3/8] 连接 MySQL 数据库...")
        step_start = time.time()
        print(f"  ├─ JDBC驱动  : {driver_class}")
        print(f"  ├─ 连接表    : zhihu_data")
        print(f"  ├─ fetchsize : 10000")

        df = spark.read.format("jdbc") \
            .option("url", db_config['url']) \
            .option("dbtable", "zhihu_data") \
            .option("user", db_config['user']) \
            .option("password", db_config['password']) \
            .option("driver", driver_class) \
            .option("fetchsize", "10000") \
            .load()
        print(f"  └─ JDBC连接建立成功 ({time.time()-step_start:.2f}s)\n")

        # ── 步骤4: 读取数据 ─────────────────────────────────
        print("[步骤 4/8] 读取并缓存数据...")
        step_start = time.time()
        df.cache()
        row_count = df.count()

        if row_count == 0:
            print("  ❌ 错误: 数据表 'zhihu_data' 没有任何记录！")
            df.unpersist()
            return

        # 打印表结构
        columns = df.columns
        print(f"  ├─ 总行数    : {row_count:,}")
        print(f"  ├─ 列数      : {len(columns)}")
        print(f"  ├─ 列名      : {columns}")
        print(f"  └─ 数据已缓存到内存 ({time.time()-step_start:.2f}s)\n")

        # ── 步骤5: 发文数量分析 ─────────────────────────────
        print("[步骤 5/8] 分析① 发文数量分布...")
        step_start = time.time()
        articles_dist = df.select("articles_count") \
            .groupBy("articles_count").count() \
            .orderBy("articles_count").limit(20).collect()
        x_data = [str(row['articles_count']) for row in articles_dist]
        y_data = [row['count'] for row in articles_dist]

        print(f"  ├─ 聚合结果  : {len(articles_dist)} 组")
        print(f"  ├─ 数据样本  :")
        for row in articles_dist[:8]:
            print(f"  │   发文{row['articles_count']}篇 → {row['count']}人")
        if len(articles_dist) > 8:
            print(f"  │   ... 共{len(articles_dist)}组")
        print(f"  └─ 完成 ({time.time()-step_start:.2f}s)\n")

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

        # ── 步骤6: 回答数量分析 ─────────────────────────────
        print("[步骤 6/8] 分析② 回答数量分布...")
        step_start = time.time()
        answer_dist = df.select("answer_count") \
            .groupBy("answer_count").count() \
            .orderBy("answer_count").limit(20).collect()
        x_data_ans = [str(row['answer_count']) for row in answer_dist]
        y_data_ans = [row['count'] for row in answer_dist]

        print(f"  ├─ 聚合结果  : {len(answer_dist)} 组")
        print(f"  ├─ 数据样本  :")
        for row in answer_dist[:8]:
            print(f"  │   回答{row['answer_count']}个 → {row['count']}人")
        if len(answer_dist) > 8:
            print(f"  │   ... 共{len(answer_dist)}组")
        print(f"  └─ 完成 ({time.time()-step_start:.2f}s)\n")

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

        # ── 步骤7: 性别分布分析 ─────────────────────────────
        print("[步骤 7/8] 分析③ 性别分布...")
        step_start = time.time()
        gender_counts = df.groupBy("gender").count().collect()
        gender_map = {1: "男", 0: "女", -1: "未知"}
        pie_data = [(gender_map.get(row['gender'], "其他"), row['count']) for row in gender_counts]

        print(f"  ├─ 分类结果  : {len(gender_counts)} 类")
        for label, count in pie_data:
            pct = count / row_count * 100
            bar = "█" * int(pct / 2)
            print(f"  │   {label}: {count:,}人 ({pct:.1f}%) {bar}")
        print(f"  └─ 完成 ({time.time()-step_start:.2f}s)\n")

        pie_gender = (
            Pie(init_opts=opts.InitOpts(theme="wonderland", width="1000px", height="600px"))
            .add("", pie_data, radius=["30%", "70%"], rosetype="radius", itemstyle_opts=opts.ItemStyleOpts(border_radius=8))
            .set_global_opts(title_opts=opts.TitleOpts(title="性别占比分布", pos_left="center"))
            .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c} ({d}%)"))
        )

        # ── 步骤8: 姓氏词云分析 ─────────────────────────────
        print("[步骤 8/8] 分析④ 姓氏词云...")
        step_start = time.time()

        # 先查看name列样本
        sample_names = df.filter(F.col("name").isNotNull()).select("name").limit(10).collect()
        print(f"  ├─ name列样本: {[row['name'] for row in sample_names]}")

        # 统计非空name数量
        name_not_null = df.filter(F.col("name").isNotNull()).count()
        print(f"  ├─ 非空name数: {name_not_null:,} / {row_count:,}")

        # 提取姓氏
        surname_counts = df.filter(
            (F.col("name").isNotNull()) & (F.length(F.col("name")) > 0)
        ).withColumn("surname", F.substring(F.col("name"), 1, 1)) \
         .filter(F.col("surname").rlike("[^\x00-\x7F]")) \
         .groupBy("surname").count().orderBy(F.desc("count")).limit(80).collect()

        print(f"  ├─ 有效姓氏数: {len(surname_counts)}")
        if surname_counts:
            print(f"  ├─ TOP10姓氏:")
            for i, row in enumerate(surname_counts[:10], 1):
                print(f"  │   {i:>2}. {row['surname']} — {row['count']:,}人")
        else:
            print(f"  ⚠ 警告: 未提取到任何有效中文姓氏！请检查name列数据编码。")
        print(f"  └─ 完成 ({time.time()-step_start:.2f}s)\n")

        wordcloud_surnames = (
            WordCloud(init_opts=opts.InitOpts(width="1000px", height="600px"))
            .add("", [(row['surname'], row['count']) for row in surname_counts], word_size_range=[25, 120], shape="circle")
            .set_global_opts(title_opts=opts.TitleOpts(title="高频姓氏词云图", pos_left="center"))
        )

        # ── 生成报告 ────────────────────────────────────────
        print("[渲染] 组装图表并生成HTML报告...")
        step_start = time.time()
        page = Page(layout=Page.SimplePageLayout)
        page.add(bar_articles, bar_answers, pie_gender, wordcloud_surnames)
        page.render(output_html)

        file_size = os.path.getsize(output_html) / 1024
        print(f"  ├─ 报告大小  : {file_size:.1f} KB")
        print(f"  └─ 渲染完成 ({time.time()-step_start:.2f}s)\n")

        # ── 最终汇总 ────────────────────────────────────────
        total_time = time.time() - total_start
        print("="*60)
        print("  🎉 分析圆满完成！")
        print("="*60)
        print(f"  总耗时      : {total_time:.2f} 秒")
        print(f"  数据总量    : {row_count:,} 条")
        print(f"  图表数量    : 4 个（发文分布、回答分布、性别分布、姓氏词云）")
        print(f"  输出文件    : {output_html}")
        print("="*60)

    except Exception as e:
        print("\n" + "!"*60)
        print(f"  ❌ 运行过程中发生错误:")
        print(f"  {str(e)}")
        print("!"*60)
        import traceback
        traceback.print_exc()
    finally:
        if 'spark' in locals():
            spark.stop()
            print("\n  Spark 引擎已停止。")

if __name__ == "__main__":
    main()
