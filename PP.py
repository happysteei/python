"""
永久投资组合策略回测 - 完全修复版
- 修复所有数据获取接口问题
- 正确处理列名
- 智能降级到模拟数据
- 详细的调试信息
"""

import akshare as ak
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'Arial Unicode MS', 'Hiragino Sans GB']
plt.rcParams['axes.unicode_minus'] = False

#%%
import akshare as ak
# bond_zh_us_rate_df = ak.bond_zh_us_rate(start_date="20161219")
# # 2. 提取中国30年国债收益率
# cn_30y_bond = bond_zh_us_rate_df[['日期', '中国国债收益率30年', '美国国债收益率30年']].copy()
# print(cn_30y_bond)

# 使用正确的接口获取该 ETF 的历史行情数据（这里才需要 symbol 参数）
# etf_hist_data = ak.fund_etf_hist_em(symbol='159659', adjust="hfq")
#
# # 查看获取到的历史数据
# print("\n纳斯达克100ETF (159659) 历史行情数据：")
# print(etf_hist_data)

# df = ak.fund_etf_hist_em(symbol="of003377", adjust="hfq")
# print(df.head())
# stock_zh_index_spot_sina_df = ak.stock_zh_index_spot_sina()
# print(stock_zh_index_spot_sina_df)
#%%
class PermanentPortfolioFixed:
    def __init__(self, start_date='20200101', end_date=None):
        """
        初始化永久投资组合
        :param start_date: 开始日期，格式 YYYYMMDD
        :param end_date: 结束日期，格式 YYYYMMDD
        """
        self.start_date = start_date
        self.end_date = end_date or datetime.now().strftime('%Y%m%d')
        self.weights = {
            'stock': 0.25,
            'bond': 0.25,
            'gold': 0.25,
            'cash': 0.25
        }
        self.data_sources = {}
    def fetch_stock_data(self):
        """获取股票数据 - 纳斯达克100ETF """
        print("正在获取股票数据（纳斯达克100ETF）...")

        try:
            print("  尝试方法: fund_etf_hist_em")
            #df = ak.stock_zh_index_daily(symbol="sh000300")
            df = ak.fund_etf_hist_em(symbol='159941', adjust="hfq")

            if df is not None and len(df) > 0:
                print(f"  数据列名: {df.columns.tolist()}")
                df['date'] = pd.to_datetime(df['日期'])
                df = df.set_index('date')[['收盘']]
                df.columns = ['stock']

                start = pd.to_datetime(self.start_date)
                end = pd.to_datetime(self.end_date)
                df = df[(df.index >= start) & (df.index <= end)]

                self.data_sources['stock'] = '纳斯达克100ETF'
                print(f"  ✓ 成功获取 {len(df)} 条数据")

                return df
        except Exception as e:
            print(f"  ✗ 方法2失败: {str(e)[:80]}")
        # 降级到模拟数据
        print("  ⚠ 使用模拟数据")
        return self._generate_mock_stock()

    def fetch_bond_data(self):
        """获取国债数据"""
        print("正在获取国债数据（中国30年国债收益率）...")

        try:
            # ===================== 【只需改这里！】 =====================
            bond_config = {
                "column": "中国国债收益率10年",  # 数据列名
                "start_date": "20160101"  # 起始日期
            }
            # ==========================================================

            print(f"  尝试方法: bond_zh_us_rate（{bond_config['column']}）")

            # 获取中美国债收益率数据
            bond_zh_us_rate_df = ak.bond_zh_us_rate(start_date=bond_config['start_date'])

            # 提取目标列
            df = bond_zh_us_rate_df[['日期', bond_config['column']]].copy()

            if df is not None and len(df) > 0:
                print(f"  数据列名: {df.columns.tolist()}")

                # 标准化日期与列名
                df['日期'] = pd.to_datetime(df['日期'])
                df = df.set_index('日期')[[bond_config['column']]]
                df.columns = ['bond']

                # 筛选日期范围
                start = pd.to_datetime(self.start_date)
                end = pd.to_datetime(self.end_date)
                df = df[(df.index >= start) & (df.index <= end)]

                if len(df) > 0:
                    self.data_sources['bond'] = bond_config['column']
                    print(f"  ✓ 成功获取 {len(df)} 条 {bond_config['column']} 数据")
                    return df
                else:
                    print(f"  ✗ 筛选后无 {bond_config['column']} 数据，请检查日期范围")

        except Exception as e:
            print(f"  ✗ 获取 {bond_config['column']} 失败: {str(e)[:80]}")

        # 降级到模拟数据
        print("  ⚠ 使用模拟数据")
        return self._generate_mock_bond()


    def fetch_gold_data(self):
        """获取黄金数据 - 上海金"""
        print("正在获取黄金数据（上海金基准价）...")

        # 方法1: spot_golden_benchmark_sge
        try:
            print("  尝试方法1: spot_golden_benchmark_sge")
            df = ak.spot_golden_benchmark_sge()

            if df is not None and len(df) > 0:
                print(f"  数据列名: {df.columns.tolist()}")

                # 查找日期和价格列
                date_col = None
                price_col = None

                for col in df.columns:
                    col_str = str(col).lower()
                    if '日期' in str(col) or 'date' in col_str:
                        date_col = col
                    if '收盘' in str(col) or 'close' in col_str or ('价' in str(col) and '收盘' in str(col)):
                        price_col = col

                # 如果没找到，使用默认列名
                if date_col is None and len(df.columns) >= 1:
                    date_col = df.columns[0]
                if price_col is None and len(df.columns) >= 2:
                    # 尝试找到包含数字的列
                    for col in df.columns[1:]:
                        if pd.api.types.is_numeric_dtype(df[col]):
                            price_col = col
                            break

                if date_col and price_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.set_index(date_col)[[price_col]]
                    df.columns = ['gold']

                    start = pd.to_datetime(self.start_date)
                    end = pd.to_datetime(self.end_date)
                    df = df[(df.index >= start) & (df.index <= end)]

                    if len(df) > 0:
                        self.data_sources['gold'] = '上海金基准价'
                        print(f"  ✓ 成功获取 {len(df)} 条数据")
                        return df
                else:
                    print(f"  ✗ 未找到合适的日期列或价格列")
        except Exception as e:
            print(f"  ✗ 方法1失败: {str(e)[:80]}")

        # 方法2: spot_hist_sge
        try:
            print("  尝试方法2: spot_hist_sge (Au99.99)")
            df = ak.spot_hist_sge(symbol="Au99.99")

            if df is not None and len(df) > 0:
                print(f"  数据列名: {df.columns.tolist()}")

                date_col = None
                price_col = None

                for col in df.columns:
                    col_str = str(col).lower()
                    if '日期' in str(col) or 'date' in col_str:
                        date_col = col
                    if '收盘' in str(col) or 'close' in col_str:
                        price_col = col

                if date_col is None and len(df.columns) >= 1:
                    date_col = df.columns[0]
                if price_col is None and len(df.columns) >= 2:
                    for col in df.columns[1:]:
                        if pd.api.types.is_numeric_dtype(df[col]):
                            price_col = col
                            break

                if date_col and price_col:
                    df[date_col] = pd.to_datetime(df[date_col])
                    df = df.set_index(date_col)[[price_col]]
                    df.columns = ['gold']

                    start = pd.to_datetime(self.start_date)
                    end = pd.to_datetime(self.end_date)
                    df = df[(df.index >= start) & (df.index <= end)]

                    if len(df) > 0:
                        self.data_sources['gold'] = 'Au99.99（上海金）'
                        print(f"  ✓ 成功获取 {len(df)} 条数据")
                        return df
        except Exception as e:
            print(f"  ✗ 方法2失败: {str(e)[:80]}")

        # 降级到模拟数据
        print("  ⚠ 使用模拟数据")
        return self._generate_mock_gold()

    def fetch_cash_data(self, benchmark_rate=0.01):
        """生成现金数据"""
        print(f"正在生成现金数据（年化收益率 {benchmark_rate*100}%）...")

        start = pd.to_datetime(self.start_date)
        end = pd.to_datetime(self.end_date)
        dates = pd.date_range(start=start, end=end, freq='D')

        daily_rate = (1 + benchmark_rate) ** (1/365) - 1
        values = [100]
        for i in range(1, len(dates)):
            values.append(values[-1] * (1 + daily_rate))

        df = pd.DataFrame({'cash': values}, index=dates)
        self.data_sources['cash'] = f'固定收益（年化{benchmark_rate*100}%）'
        print(f"  ✓ 成功生成 {len(df)} 条数据")
        return df


    def merge_data(self):
        """合并所有资产数据"""
        print("\n" + "="*60)
        print("开始获取各类资产数据...")
        print("="*60 + "\n")

        stock_df = self.fetch_stock_data()
        bond_df = self.fetch_bond_data()
        gold_df = self.fetch_gold_data()
        cash_df = self.fetch_cash_data()

        print("\n正在合并数据...")
        df_list = [stock_df, bond_df, gold_df, cash_df]
        df_list = [df for df in df_list if not df.empty]

        if not df_list:
            raise ValueError("没有成功获取任何资产数据")

        # 使用外连接，然后前向填充
        merged_df = pd.concat(df_list, axis=1, join='outer')
        merged_df = merged_df.fillna(method='ffill').fillna(method='bfill')
        merged_df = merged_df.sort_index()

        # 调整权重
        available_assets = merged_df.columns.tolist()
        missing_assets = [a for a in ['stock', 'bond', 'gold', 'cash'] if a not in available_assets]

        if missing_assets:
            print(f"\n⚠ 警告: 以下资产数据缺失: {', '.join(missing_assets)}")
            print("  权重将按可用资产重新分配...")
            n_assets = len(available_assets)
            new_weight = 1.0 / n_assets
            self.weights = {asset: new_weight for asset in available_assets}

        print(f"\n✓ 数据合并完成:")
        print(f"  交易日数: {len(merged_df)}")
        print(f"  日期范围: {merged_df.index[0].date()} 至 {merged_df.index[-1].date()}")
        print(f"  资产类别: {', '.join(merged_df.columns.tolist())}")

        print(f"\n数据来源:")
        for asset in available_assets:
            source = self.data_sources.get(asset, '未知')
            weight = self.weights.get(asset, 0) * 100
            print(f"  {asset:8s} ({weight:.0f}%) - {source}")

        return merged_df

    def calculate_returns(self, price_df, rebalance_freq='yearly'):
        """
        计算收益率（支持定期再平衡，真正实现PP策略）
        :param price_df: 资产价格DataFrame
        :param rebalance_freq: 再平衡周期 'yearly'/'half_yearly'/'quarterly'
        :return: portfolio_value（组合净值）, returns（日收益率）
        """
        # 1. 初始化参数
        n_assets = len(self.weights)
        target_weights = np.array([self.weights[col] for col in price_df.columns])
        prices = price_df.copy()
        portfolio_values = []
        dates = []

        # 初始资金（100元）
        initial_capital = 100.0
        current_capital = initial_capital
        # 初始持仓：按目标权重分配
        current_holdings = initial_capital * target_weights / prices.iloc[0].values

        # 2. 确定再平衡日期
        prices['year'] = prices.index.year
        prices['half_year'] = prices.index.year * 10 + (prices.index.month > 6) + 1
        prices['quarter'] = prices.index.year * 10 + prices.index.quarter

        if rebalance_freq == 'yearly':
            rebalance_dates = prices.groupby('year').head(1).index
        elif rebalance_freq == 'half_yearly':
            rebalance_dates = prices.groupby('half_year').head(1).index
        elif rebalance_freq == 'quarterly':
            rebalance_dates = prices.groupby('quarter').head(1).index
        else:
            raise ValueError("rebalance_freq 仅支持 yearly/half_yearly/quarterly")

        # 3. 逐行计算（含再平衡）
        for i in range(len(prices)):
            current_date = prices.index[i]
            current_price = prices.iloc[i].values[:n_assets]

            # 计算当前持仓市值
            current_capital = np.sum(current_holdings * current_price)
            portfolio_values.append(current_capital)
            dates.append(current_date)

            # 再平衡逻辑：到再平衡日期 → 调回目标权重
            if current_date in rebalance_dates and i > 0:
                current_holdings = current_capital * target_weights / current_price

        # 4. 生成组合净值与收益率
        portfolio_value = pd.Series(portfolio_values, index=dates, name='portfolio')
        returns = portfolio_value.pct_change().fillna(0)

        print(f"  ✓ 已实现{rebalance_freq}再平衡，真正符合永久投资组合规则")
        return portfolio_value, returns

    def calculate_drawdown(self, portfolio_value):
        """计算回撤"""
        cumulative_max = portfolio_value.expanding().max()
        drawdown = (portfolio_value - cumulative_max) / cumulative_max
        return drawdown

    def calculate_metrics(self, portfolio_value, returns):
        """计算投资组合指标"""
        total_return = (portfolio_value.iloc[-1] / portfolio_value.iloc[0] - 1) * 100

        days = (portfolio_value.index[-1] - portfolio_value.index[0]).days
        annual_return = ((portfolio_value.iloc[-1] / portfolio_value.iloc[0]) ** (365/days) - 1) * 100

        annual_volatility = returns.std() * np.sqrt(252) * 100

        risk_free_rate = 0.02
        sharpe_ratio = (annual_return/100 - risk_free_rate) / (annual_volatility/100) if annual_volatility > 0 else 0

        drawdown = self.calculate_drawdown(portfolio_value)
        max_drawdown = drawdown.min() * 100

        calmar_ratio = annual_return / abs(max_drawdown) if max_drawdown != 0 else 0
        win_rate = (returns > 0).sum() / len(returns) * 100 if len(returns) > 0 else 0

        metrics = {
            '总收益率': f'{total_return:.2f}%',
            '年化收益率': f'{annual_return:.2f}%',
            '年化波动率': f'{annual_volatility:.2f}%',
            '夏普比率': f'{sharpe_ratio:.2f}',
            '最大回撤': f'{max_drawdown:.2f}%',
            '卡玛比率': f'{calmar_ratio:.2f}',
            '胜率': f'{win_rate:.2f}%',
            '起始日期': str(portfolio_value.index[0].date()),
            '结束日期': str(portfolio_value.index[-1].date()),
            '交易天数': days
        }

        return metrics

    def plot_results(self, price_df, portfolio_value, returns):
        """绘制结果图表"""
        drawdown = self.calculate_drawdown(portfolio_value)

        fig = plt.figure(figsize=(18, 12))

        # 1. 各资产价格走势
        ax1 = plt.subplot(3, 3, 1)
        normalized_df = price_df / price_df.iloc[0] * 100
        colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
        for i, col in enumerate(normalized_df.columns):
            ax1.plot(normalized_df.index, normalized_df[col],
                    label=f'{col.upper()} ({self.weights.get(col, 0)*100:.0f}%)',
                    linewidth=2, color=colors[i % len(colors)])
        ax1.set_title('各资产价格走势（归一化，基期=100）', fontsize=13, fontweight='bold')
        ax1.set_xlabel('日期')
        ax1.set_ylabel('价格指数')
        ax1.legend(fontsize=9)
        ax1.grid(True, alpha=0.3)

        # 2. 组合净值
        ax2 = plt.subplot(3, 3, 2)
        ax2.plot(portfolio_value.index, portfolio_value, color='#2E4057', linewidth=2.5)
        ax2.fill_between(portfolio_value.index, portfolio_value, alpha=0.3, color='#6C9BCF')
        ax2.set_title('永久投资组合净值曲线', fontsize=13, fontweight='bold')
        ax2.set_xlabel('日期')
        ax2.set_ylabel('净值')
        ax2.grid(True, alpha=0.3)

        # 3. 累积收益
        ax3 = plt.subplot(3, 3, 3)
        cumulative_return = (portfolio_value / portfolio_value.iloc[0] - 1) * 100
        ax3.plot(cumulative_return.index, cumulative_return, color='#27AE60', linewidth=2.5)
        ax3.fill_between(cumulative_return.index, cumulative_return, alpha=0.3, color='#82E0AA')
        ax3.set_title('累积收益率曲线', fontsize=13, fontweight='bold')
        ax3.set_xlabel('日期')
        ax3.set_ylabel('累积收益率 (%)')
        ax3.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax3.grid(True, alpha=0.3)

        # 4. 回撤
        ax4 = plt.subplot(3, 3, 4)
        ax4.fill_between(drawdown.index, drawdown * 100, alpha=0.5, color='#E74C3C')
        ax4.plot(drawdown.index, drawdown * 100, color='#943126', linewidth=2)
        if drawdown.min() < 0:
            max_dd_idx = drawdown.idxmin()
            ax4.scatter([max_dd_idx], [drawdown.min() * 100], color='red', s=100, zorder=5,
                       label=f'最大回撤: {drawdown.min()*100:.2f}%')
        ax4.set_title('回撤曲线', fontsize=13, fontweight='bold')
        ax4.set_xlabel('日期')
        ax4.set_ylabel('回撤 (%)')
        ax4.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax4.legend(fontsize=8)
        ax4.grid(True, alpha=0.3)

        # 5. 收益率分布
        ax5 = plt.subplot(3, 3, 5)
        ax5.hist(returns * 100, bins=60, alpha=0.7, color='#8E44AD', edgecolor='black', linewidth=0.5)
        ax5.axvline(x=returns.mean() * 100, color='red', linestyle='--', linewidth=2,
                   label=f'均值: {returns.mean()*100:.3f}%')
        ax5.axvline(x=0, color='black', linestyle='-', linewidth=1, alpha=0.5)
        ax5.set_title('日收益率分布', fontsize=13, fontweight='bold')
        ax5.set_xlabel('日收益率 (%)')
        ax5.set_ylabel('频数')
        ax5.legend(fontsize=9)
        ax5.grid(True, alpha=0.3)

        # 6. 配置饼图
        ax6 = plt.subplot(3, 3, 6)
        labels = [f'{asset.upper()}\n{weight*100:.0f}%'
                 for asset, weight in self.weights.items()]
        sizes = [weight * 100 for weight in self.weights.values()]
        colors_pie = ['#FF6B6B', '#4ECDC4', '#FFD93D', '#95E1D3'][:len(sizes)]
        explode = tuple([0.05] * len(sizes))
        wedges, texts, autotexts = ax6.pie(sizes, explode=explode, labels=labels, colors=colors_pie,
               autopct='%1.0f%%', shadow=True, startangle=90)
        for autotext in autotexts:
            autotext.set_color('white')
            autotext.set_fontsize(10)
        ax6.set_title('资产配置', fontsize=13, fontweight='bold')

        # 7. 滚动收益
        ax7 = plt.subplot(3, 3, 7)
        rolling_return = portfolio_value.pct_change(30) * 100
        ax7.plot(rolling_return.index, rolling_return, color='#3498DB', linewidth=1.5, alpha=0.8)
        ax7.axhline(y=0, color='black', linestyle='--', alpha=0.5)
        ax7.fill_between(rolling_return.index, rolling_return,
                        where=(rolling_return > 0), alpha=0.3, color='green', interpolate=True)
        ax7.fill_between(rolling_return.index, rolling_return,
                        where=(rolling_return <= 0), alpha=0.3, color='red', interpolate=True)
        ax7.set_title('滚动30日收益率', fontsize=13, fontweight='bold')
        ax7.set_xlabel('日期')
        ax7.set_ylabel('收益率 (%)')
        ax7.grid(True, alpha=0.3)

        # 8. 各资产贡献
        ax8 = plt.subplot(3, 3, 8)
        contributions = {}
        for asset in normalized_df.columns:
            asset_return = (normalized_df[asset].iloc[-1] / normalized_df[asset].iloc[0] - 1)
            contributions[asset.upper()] = asset_return * self.weights.get(asset, 0) * 100

        bars = ax8.bar(contributions.keys(), contributions.values(),
                      color=colors[:len(contributions)], alpha=0.7, edgecolor='black', linewidth=1.5)
        ax8.set_title('各资产收益贡献', fontsize=13, fontweight='bold')
        ax8.set_ylabel('贡献度 (%)')
        ax8.grid(True, alpha=0.3, axis='y')

        for bar in bars:
            height = bar.get_height()
            ax8.text(bar.get_x() + bar.get_width()/2., height,
                    f'{height:.2f}%', ha='center', va='bottom' if height > 0 else 'top', fontsize=9)

        # 9. 数据来源信息
        ax9 = plt.subplot(3, 3, 9)
        ax9.axis('off')

        info_text = "数据来源\n" + "="*35 + "\n\n"
        for asset in self.weights.keys():
            source = self.data_sources.get(asset, '未知')
            weight = self.weights.get(asset, 0) * 100
            # 缩短来源文字以适应显示
            if len(source) > 25:
                source = source[:22] + "..."
            info_text += f"{asset.upper():6s} ({weight:.0f}%)\n{source}\n\n"

        info_text += "="*35 + "\n"
        info_text += f"生成时间:\n{datetime.now().strftime('%Y-%m-%d %H:%M')}"

        ax9.text(0.05, 0.5, info_text, transform=ax9.transAxes,
                fontsize=9, verticalalignment='center',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

        plt.tight_layout(pad=2.0)

        output_path = 'permanent_portfolio_analysis.png'
        plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')
        print(f"\n✓ 图表已保存: {output_path}")

        return fig

    def run_analysis(self):
        """运行完整分析"""
        try:
            price_df = self.merge_data()

            print("\n计算投资组合收益...")
            portfolio_value, returns = self.calculate_returns(price_df, rebalance_freq='yearly')

            print("计算投资组合指标...")
            metrics = self.calculate_metrics(portfolio_value, returns)

            print("\n" + "="*60)
            print("永久投资组合分析结果")
            print("="*60)
            for key, value in metrics.items():
                print(f"{key:12s}: {value}")
            print("="*60 + "\n")

            print("正在生成可视化图表...")
            self.plot_results(price_df, portfolio_value, returns)

            output_data_path = 'portfolio_data.csv'
            result_df = pd.DataFrame({
                '组合净值': portfolio_value,
                '日收益率': returns,
                '累积收益率': (portfolio_value / portfolio_value.iloc[0] - 1) * 100,
                '回撤': self.calculate_drawdown(portfolio_value) * 100
            })
            result_df.to_csv(output_data_path, encoding='utf-8-sig')
            print(f"✓ 详细数据已保存: {output_data_path}")

            print("\n" + "="*60)
            print("✓ 分析完成！")
            print("="*60)

            return metrics, price_df, portfolio_value, returns

        except Exception as e:
            print(f"\n✗ 分析过程出错: {e}")
            import traceback
            traceback.print_exc()
            return None, None, None, None


def main():
    """主函数"""
    print("="*60)
    print(" "*15 + "永久投资组合策略回测系统")
    print(" "*15 + "Permanent Portfolio Analysis")
    print("="*60)
    print("\n策略说明：")
    print("  - 25% 股票（沪深300指数）")
    print("  - 25% 国债（中债新综合指数）")
    print("  - 25% 黄金（上海金基准价）")
    print("  - 25% 现金（年化2%收益率）")
    print("\n注意：")
    print("  - 代理设置可能影响数据获取")
    print("="*60 + "\n")

    # 创建投资组合实例
    portfolio = PermanentPortfolioFixed(start_date='20160101')

    # 运行分析
    portfolio.run_analysis()


if __name__ == "__main__":
    main()
