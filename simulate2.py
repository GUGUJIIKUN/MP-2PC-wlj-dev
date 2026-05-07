# 我们这个文件通过python来模拟Tleap 和 T2pc 的功能, 为混合做准备, 我们现在假设 n 是一个事务偏离majority affinity primary 的页面数量
# X 是网络 1 hop 的耗时, Y 是写日志的开销
# Tleap = αlpha * X * n + Y 
# T2pc = (beta + i) * X + 3 * Y

import matplotlib.pyplot as plt
import numpy as np

def simulate_and_plot():
    # 1. 设定公式中的常数参数 (可以根据你的实际环境做调整)
    alpha_list = [2, 3, 4]   # leap 需要的所有权转移开销系数的多个可能值
    beta = 5                 # 2PC 需要的网络开销系数
    i = 5                    # 2PC 附加的开销系数
    X = 100                  # 基础耗时 (例如网络 1 hop 耗时)
    Y = 100                  # 写日志的开销

    # 2. 生成 n 取值范围，比如从 1 到 10，代表偏离的页面数量
    n_values = np.arange(1, 11)

    # 3. 计算 T2pc 曲线 
    # T2pc = (beta + i) * X + 3 * Y
    # 注意：根据当前公式，T2pc 的开销与 n 无关，所以它是一条水平直线
    T_2pc_val = (beta + i) * X + 3 * Y
    T_2pc = np.full_like(n_values, T_2pc_val)

    # 4. 开始绘制单张折线图
    plt.figure(figsize=(10, 6))
    
    # 绘制多条 T_leap 曲线和它们对应的交点
    colors = ['blue', 'green', 'purple']
    for idx, alpha in enumerate(alpha_list):
        T_leap = alpha * X * n_values + Y
        plt.plot(n_values, T_leap, label=f'T_leap (α={alpha})', color=colors[idx], linewidth=2.5, linestyle='-')
        
        # 计算两线交点 (当 T_leap == T_2pc 时的平衡点)
        # alpha * X * n + Y = T_2pc_val  =>  n = (T_2pc_val - Y) / (alpha * X)
        intersect_n = (T_2pc_val - Y) / (alpha * X)
        if intersect_n <= 10:  # 只有交点在绘图范围内时才画辅助线
            plt.axvline(x=intersect_n, color=colors[idx], linestyle=':', alpha=0.7, label=f'Intersection α={alpha} (n={intersect_n:.2f})')

    plt.plot(n_values, T_2pc, label=f'T_2pc (β={beta}, i={i})', color='red', linewidth=2.5, linestyle='--')

    # 补充图表标题、轴标签及网格
    plt.title('Simulation of T_leap vs T_2pc Cost with varying n', fontsize=14)
    plt.xlabel('n (Number of pages deviating from majority affinity)', fontsize=12)
    plt.ylabel('Cost Base', fontsize=12)
    
    # 强制让 x 轴只显示整数点
    plt.xticks(n_values)
    
    plt.legend(fontsize=12)
    plt.grid(True, linestyle='--', alpha=0.7)

    # 6. 保存到本地并展示
    plt.savefig('simulate_result2.png', dpi=300)
    print("图表已保存为 simulate_result2.png")
    
if __name__ == "__main__":
    simulate_and_plot() 

