A Python tool to calculate Austrian tax on capital gains based on reports from Interactive Brokers, Freedom Finance, Revolut and Wise.

## Interaktive Brokers

When you generate a Flex Query in IB, extend the reported timeline by a few months if possible, especially if you have REITs in your portfolio. For example, set the range from 2024-01-01 to 2025-03-01 instead of ending on 2024-12-31.

The reason for this is that brokers sometimes need to retrospectively adjust the withholding tax on dividends. For example, I have noticed several cases with REITs where a dividend was credited, and the standard 15% tax was withheld. However, in February of the following year, the withholding tax was canceled out and replaced with a lower amount. This is likely due to a specific REIT dividend structure.

Example:
Dividend: 4.35 -> Withholding tax: -0.65 (15%) -> Reversal: +0.65 -> Final withholding: -0.48.
