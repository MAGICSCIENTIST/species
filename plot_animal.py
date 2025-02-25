import matplotlib.pyplot as plt


# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei']  # 使用黑体 SimHei 显示中文
plt.rcParams['axes.unicode_minus'] = False    # 解决负号 '-' 显示为方块的问题

# 数据
protection_level = ["一级"] * 8 + ["二级"] * 7
endangerment_level = ["区域灭绝", "数据缺乏", "无危", "易危", "极危", "濒危", "近危", "野外灭绝",
                      "数据缺乏", "无危", "易危", "未评定", "极危", "濒危", "近危"]
total = [2, 137, 4, 10, 44, 30, 6, 2, 687, 13, 19, 1, 7, 15, 11]
families = [2, 50, 2, 7, 16, 13, 4, 2, 154, 5, 12, 1, 4, 10, 5]

# 使用 zip 函数组合两个列表
combined_series = [f"{protection}-{endangerment}" for protection, endangerment in zip(protection_level, endangerment_level)]

# 创建散点图
plt.figure(figsize=(10, 6))

# 为不同的保护等级分别绘制散点图，并设置颜色和图例标签
colors = {'一级': 'red', '二级': 'blue'}

for protection in set(protection_level):
    indices = [i for i, p in enumerate(protection_level) if p == protection]
    plt.scatter([total[i] for i in indices], [families[i] for i in indices], 
                c=colors[protection], s=100, alpha=0.6, label=protection)

# 添加标签
for i in range(len(total)):
    plt.text(total[i] + 5, families[i], endangerment_level[i], fontsize=9)

# 设置图表标题和坐标轴标签
plt.title("濒危等级与总计和科类的散点图", fontsize=14)
plt.xlabel("物种", fontsize=12)
plt.ylabel("科类", fontsize=12)

# 显示图例，表示不同的保护等级
plt.legend(loc="upper left")

# 显示图表
plt.grid(True)
plt.show()
