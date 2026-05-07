# 我们这个文件通过python来模拟Tleap 和 T2pc 的功能, 为混合做准备
# Tleap = αlpha * X * P(R) * (2 - P(R)) 
# T2pc = (beta + i) * X * [1 - (1 - P(R)) ^ n]

import matplotlib.pyplot as plt
import numpy as np

def simulate_and_plot():
    # 1. 设定公式中的常数参数 (可以根据你的实际环境做调整)
    alpha = 2        # leap 需要的所有权转移开销
    beta = 5         # 2PC 需要的网络开销
    i = 1            # 2PC 附加的开销系数
    X = 100          # 基础耗时或事务规模
    n_values = [2, 3, 5, 8]  # 定义多个不同的 n 值做对比
    log = 100        # 写日志开销

    # 2. 生成 P(R) 取值范围从 0 到 1 的 100 个散点 (代表 Remote 请求的概率 0% -> 100%)
    P_R = np.linspace(0, 1, 100)

    # 3. 创建 2x2 的子图网格 (因为有 4 个不同的 n 值)
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Simulation of T_leap vs T_2pc Cost with varying n', fontsize=16)
    
    # 扁平化以便于循环遍历
    axes = axes.flatten()

    for idx, n in enumerate(n_values):
        ax = axes[idx]
        
        # 计算该 n 值下的 T_leap 曲线
        T_leap = n * alpha * X * P_R * (2 - P_R) + log
        # 用同一颜色的实线表示 T_leap
        ax.plot(P_R, T_leap, label=f'T_leap (n={n})', color='blue', linewidth=2.5, linestyle='-')
        
        # 计算该 n 值下的 T_2pc 曲线
        T_2pc = (beta + i) * X * (1 - (1 - P_R)**n) + 3 * log
        # 用同一颜色的虚线表示 T_2pc
        ax.plot(P_R, T_2pc, label=f'T_2pc (n={n})', color='red', linewidth=2.5, linestyle='--')
        
        # 为每个子图设置标题、轴标签及网格
        ax.set_title(f'Comparison at n = {n}', fontsize=13)
        ax.set_xlabel('P(R)', fontsize=11)
        ax.set_ylabel('Cost Base', fontsize=11)
        ax.legend(fontsize=11)
        ax.grid(True, linestyle='--', alpha=0.7)

    # 调整布局防止子标题重叠
    plt.tight_layout()
    plt.subplots_adjust(top=0.92)

    # 4. 保存到本地并展示
    plt.savefig('simulate_result.png', dpi=300)
    print("各 n 值子图表已保存为 simulate_result.png")
    
if __name__ == "__main__":
    simulate_and_plot() 

