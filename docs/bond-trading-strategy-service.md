# Bond Trading Strategy Service

## Overview

The bond trading strategy service is a service that hosts bond trading strategies that can be backtested
and then used to trade directly on the DORA platform.

Currently, DORA provides a traditional trading UI allowing users to trade manually on the platform. Trading
decisions are made by the user, and the user is responsible for placing orders when they are ready. While
DORA provides an API for users to create their own trading strategies, the users themselves would need to
know how to code and write trading strategies.

DORA will create a number of bond trading strategies that their users can customize with their own values
for the parameters that drive the trading strategy. They can then back test these strategies, and then
set the strategies to run by themselves to trade directly on the DORA platform on behalf of the users.

The bond trading strategy service will expose a MCP service that will allow an AI agent to interact with
the service. This will allow the user to talk to the service to tweak the parameters of the strategies,
run backtests and start and stop the strategies from running on their behalf.

Traditionally, the trading platform may provide a UI for users to interact with the strategies, filling out
form fields and clicking buttons to run backtests and start and stop the strategies. This can still be done,
allowing users to interact with the UI directly, but providing the MCP service allows DORA to provide an AI
agent interface on top of the traditional UI.

Additionally, the MCP service will allow users to integrate the trading strategy service directly into their
own preferred AI agent tooling. For example, users who prefer to use a tool like OpenAI's ChatGPT can use
the MCP service to interact with the trading strategy service and run backtests and start and stop the
strategies instead of using the traditional UI.

### Example

Given a bond trading strategy service, with an MCP service, and an AI agent interface,
an example of the user's interaction with the agent could be:

```text
> what trading strategies are available?

Available trading strategies:
- Strategy 1 : Description of strategy 1, risk profile, parameters
- Strategy 2 : Description of strategy 2, risk profile, parameters
- Strategy 3 : Description of strategy 3, risk profile, parameters

> Tell me more about strategy 1

Description of strategy 1:
- Risk profile: Moderate
- Parameters: Parameter 1, Parameter 2, Parameter 3
- Description: This strategy is designed to provide a balance between risk and return, with a focus on Parameter 1.

> What values can I set for each parameter?

Available parameter values:
- Parameter 1: Range 1-10, default 5
- Parameter 2: Range 0.1-1.0, default 0.5
- Parameter 3: Range 100-1000, default 500

> Run a backtest market <MKT1> using strategy1 with Parameter 1 set to 7, Parameter 2 set to 0.8, and Parameter 3 set to 500, for the past year, and return the results

Backtest results:
- Sharpe ratio: 1.23
- Maximum drawdown: -5%
- Annualized return: 10%
- Cumulative return: 15%
- Trading days: 252
- Trades: 100
- Win rate: 60%
- Loss rate: 40%

> Rerun strategy 1 but with Parameter 1 set to 8

Backtest results:
- Sharpe ratio: 1.18
- Maximum drawdown: -4.5%
- Annualized return: 9.5%
- Cumulative return: 14.5%
- Trading days: 252
- Trades: 95
- Win rate: 58%
- Loss rate: 42%

> Start trading on market <MKT1> using strategy 1 with Parameter 1 set to 7, Parameter 2 set to 0.8, and Parameter 3 set to 500. Run the strategy for the next 30 trading days.

Started trading on market <MKT1> strategy 1 with Parameter 1 set to 7, Parameter 2 set to 0.8, and Parameter 3 set to 500.

> What strategies are currently running?

You are currently trading strategy 1 with Parameter 1 set to 7, Parameter 2 set to 0.8, and Parameter 3 set to 500.
The strategy has been running since <some-date> and will continue to run for another <some-number-of-days>.
You have executed the following trades:
- Trade 1: <side> <quantity> <asset> at <price>
- Trade 2: <side> <quantity> <asset> at <price>
- Trade 3: <side> <quantity> <asset> at <price>

Unrealized profit/loss: <profit-loss>
Realized profit/loss: <realized-profit-loss>

> Stop trading on market <MKT1> strategy 1

You currently have <number-of-trading-days> trading days remaining. Are you sure you want to stop trading?
Type 'yes' to confirm.

> yes

You currently have <number-of-open-orders> open orders. Do you want to cancel them?
Type 'yes' to confirm.

> yes

Trading on market <MKT1> has been stopped.
```

While this is a simplified example, it illustrates the general idea of how the AI agent can interact with
the bond trading strategy service. As we add more strategies and features to the service, the AI agent
interface will evolve to provide a more intuitive and user-friendly experience.

## MCP Usage

The MCP server exposes both raw JSON strategy-run tools and question-oriented strategy-run tools.

Prefer these tools for questions about runs:

- `strategy_run_status` for questions like:
  - "What strategy runs are active right now?"
  - "What is paused?"
  - "Summarize current strategy runs"
- `strategy_run_describe` for questions like:
  - "Tell me about run `<run-id>`"
  - "Why is this run paused?"
  - "What config is this run using?"

Use these tools when raw API output is needed instead:

- `strategy_run_list` to fetch the full run list as JSON
- `strategy_run_get` to fetch one run as JSON

Example prompt to an MCP-aware agent:

```text
What strategy runs are active right now?
```

Recommended MCP tool:

```text
strategy_run_status
```

Example prompt to inspect one run:

```text
Tell me about run 22222222-2222-2222-2222-222222222222
```

Recommended MCP tool:

```text
strategy_run_describe
```

We can also provide a traditional UI for users to interact with the strategies, filling out form fields
and clicking buttons to run backtests and start and stop the strategies.

The trading strategy service will provide users who want to trade bonds, but without the experience or
expertise of bond trading, to get involved with bond trading.
