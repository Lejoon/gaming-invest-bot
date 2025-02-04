#!/usr/bin/env python3
"""
Refactored steam_chart.py with PS aggregation fix and updated quarter labeling.
Assumes API data is quarterly (the data point “date” is the quarter’s start date).
"""

import io
import datetime
import calendar
import re
from dateutil.relativedelta import relativedelta
import matplotlib.pyplot as plt
import numpy as np
import pytz  # for timezone support in embed timestamp
import discord
from discord import Embed
from matplotlib import rcParams
import asyncio

# Import API functions from vgi_api.py
from vgi_api import (
    search_game,
    get_game_details,
    get_sales_data_steam,
    get_logos_data,
    get_quick_stats,
    get_sales_data_playstation,
)

# ----------------------------------------------------------------------
# Helper Functions for Data Processing and Formatting
# ----------------------------------------------------------------------

def has_playstation(details_data):
    """
    Returns True if details_data indicates PlayStation availability.
    Expects details_data to have a key "platformAvailability" with a "playstation" value.
    """
    return bool(details_data.get("platformAvailability", {}).get("playstation"))

def parse_date(date_str):
    """Parse a date string in YYYY-MM-DD format."""
    return datetime.datetime.strptime(date_str, "%Y-%m-%d").date()

def get_calendar_quarter(date_obj):
    """Return the calendar quarter (1, 2, 3, or 4) for a given date."""
    return ((date_obj.month - 1) // 3) + 1

def quarter_label(base_date, offset):
    """
    Given a base_date and a quarter offset (as computed by quarter_index),
    return a label like "2024 Q3".
    """
    base_q = get_calendar_quarter(base_date)
    total_quarter = (base_q - 1) + offset
    year = base_date.year + (total_quarter // 4)
    quarter = (total_quarter % 4) + 1
    return f"{year} Q{quarter}"

def quarter_end(date_obj):
    """
    Given a date, return the last day of its calendar quarter.
    """
    q = get_calendar_quarter(date_obj)
    last_month = q * 3
    last_day = calendar.monthrange(date_obj.year, last_month)[1]
    return datetime.date(date_obj.year, last_month, last_day)

def abs_quarter(date_obj):
    """
    Map a date to an absolute quarter index.
    """
    return date_obj.year * 4 + (get_calendar_quarter(date_obj) - 1)

def format_value(val_millions):
    """
    Format a value (in millions) as thousands (k) if under 1, or as millions (m) with two decimals.
    """
    if val_millions < 1:
        return f"{int(round(val_millions * 1000))}k"
    else:
        return f"{val_millions:.2f}m"

def format_reviews(review_count):
    """
    Format review counts: below 1,000 as an integer, otherwise in k with one decimal.
    """
    if review_count < 1000:
        return str(review_count)
    else:
        return f"{review_count/1000:.1f}k"

def format_revenue(revenue):
    """
    Format revenue similar to format_value but with a dollar sign.
    """
    return "$" + format_value(revenue)

def quarter_index(date, base_date):
    """
    Return the number of complete quarters between base_date and date.
    """
    return (date.year - base_date.year) * 4 + (get_calendar_quarter(date) - get_calendar_quarter(base_date))


# ----------------------------------------------------------------------
# 1. Data Retrieval
# ----------------------------------------------------------------------
def retrieve_game_data(game_name):
    """
    Performs all API calls to retrieve raw data.
    Returns a dictionary with:
      - game_name: Canonical game name.
      - game_slug: URL-friendly version of the game name.
      - similar_titles: List of similar game names (if any).
      - details: Game details data.
      - sales_data: Steam sales data.
      - quick_data: Quick stats data.
      - logos: Logos data.
      - has_ps: Boolean flag for PlayStation availability.
      - ps_sales: (Optional) PlayStation sales data if available.
    """
    # --- Game Search and Canonical Name Determination ---
    search_results = search_game(game_name)
    similar_titles = []
    if search_results:
        top_hit = search_results[0]
        canonical_game_name = top_hit["name"]
        if len(search_results) > 1:
            similar_titles = [hit["name"] for hit in search_results[1:]]
    else:
        canonical_game_name = game_name

    # Generate URL-friendly slug
    game_slug = re.sub(r'[^a-z0-9]+', '-', canonical_game_name.lower()).strip('-')

    # --- Data Retrieval ---
    details = get_game_details(game_slug)
    sales_data = get_sales_data_steam(game_slug)
    quick_data = get_quick_stats(game_slug)
    logos = get_logos_data(game_slug)

    # Check for PlayStation data availability.
    ps_sales = None
    ps_available = has_playstation(details)
    if ps_available:
        try:
            ps_sales = get_sales_data_playstation(game_slug)
        except Exception as e:
            ps_available = False
            print(f"Error retrieving PlayStation EA date: {e}")

    return {
        "game_name": canonical_game_name,
        "game_slug": game_slug,
        "similar_titles": similar_titles,
        "details": details,
        "sales_data": sales_data,
        "quick_data": quick_data,
        "logos": logos,
        "has_ps": ps_available,
        "ps_sales": ps_sales,
    }


# ----------------------------------------------------------------------
# 2. Data Ordering & Aggregation
# ----------------------------------------------------------------------
def aggregate_sales_data(raw_data):
    """
    Process and aggregate raw sales data.
    Computes quarter indices, aggregates quarterly and cumulative data,
    and optionally re-aggregates into yearly data if there are 12+ quarters.
    
    Returns a dictionary with:
      - positions: X-axis positions (quarter or year indexes)
      - aggregated_labels: Labels for the X-axis.
      - steam_cum_units, steam_quarter_units, steam_cum_revenue, steam_quarter_revenue: Aggregated arrays.
      - (if available) ps_cum_units, ps_quarter_units.
      - totals: A dictionary with total_units, total_revenue, avg_unit_price.
      - is_plotable: Boolean indicating if there is enough data to plot.
    """
    sales_data = raw_data["sales_data"]
    has_ps = raw_data["has_ps"]
    ps_sales = raw_data["ps_sales"]

    # Determine release dates and base date
    steam_release_date = parse_date(sales_data["steam"]["eaDate"])
    ps_release_date = None
    if has_ps and ps_sales:
        try:
            ps_release_date = parse_date(ps_sales["playstation"]["eaDate"])
        except Exception as e:
            has_ps = False
            print(f"Error processing PlayStation EA date: {e}")

    base_date = steam_release_date
    if has_ps and ps_release_date is not None:
        base_date = min(steam_release_date, ps_release_date)

    # --- Process Steam Data ---
    steam_points = sales_data["steam"]["dataPoints"]
    steam_agg = {}
    for point in steam_points:
        # Since the API data is quarterly, simply parse the provided date.
        actual_date = parse_date(point["date"])
        point["actual_date"] = actual_date
        point["q_index"] = quarter_index(actual_date, base_date)
        q = point["q_index"]
        if q not in steam_agg:
            steam_agg[q] = {
                "quarterly_units": 0,
                "quarterly_revenue": 0,
                "cumulative_units": None,
                "cumulative_revenue": None,
                "latest_date": None
            }
        steam_agg[q]["quarterly_units"] += point["units"]
        steam_agg[q]["quarterly_revenue"] += point["revenue"]
        if (steam_agg[q]["latest_date"] is None) or (point["actual_date"] > steam_agg[q]["latest_date"]):
            steam_agg[q]["latest_date"] = point["actual_date"]
            steam_agg[q]["cumulative_units"] = point["cumulative_units"]
            steam_agg[q]["cumulative_revenue"] = point["cumulative_revenue"]

    # --- Process PlayStation Data (if available) ---
    ps_agg = {}
    if has_ps and ps_sales:
        try:
            ps_points = ps_sales["playstation"]["dataPoints"]
            # First loop: compute actual dates and quarter indices.
            for point in ps_points:
                actual_date = parse_date(point["date"])
                point["actual_date"] = actual_date
                point["q_index"] = quarter_index(actual_date, base_date)
            # Second loop: aggregate the data.
            for point in ps_points:
                q = point["q_index"]
                if q not in ps_agg:
                    ps_agg[q] = {
                        "quarterly_units": 0,
                        "cumulative_units": None,
                        "latest_date": None,
                    }
                ps_agg[q]["quarterly_units"] += point["units"]
                if (ps_agg[q]["latest_date"] is None) or (point["actual_date"] > ps_agg[q]["latest_date"]):
                    ps_agg[q]["latest_date"] = point["actual_date"]
                    ps_agg[q]["cumulative_units"] = point["cumulative_units"]
        except Exception as e:
            has_ps = False
            print(f"Error processing PlayStation data: {e}")

    # Check if PS data has valid values.
    if has_ps:
        ps_quarters = set(ps_agg.keys())
        if not ps_quarters or all(np.isnan(ps_agg[q]["cumulative_units"]) for q in ps_quarters):
            has_ps = False

    # ── NEW: Adjust PlayStation quarterly units to be incremental ──
    if has_ps:
        # Sort the quarters and compute incremental (quarterly) units by subtracting the previous quarter's cumulative.
        sorted_ps_quarters = sorted(ps_agg.keys())
        previous_cumulative = 0
        for q in sorted_ps_quarters:
            current_cumulative = ps_agg[q].get("cumulative_units", 0)
            # Compute incremental units as the difference.
            ps_agg[q]["quarterly_units"] = current_cumulative - previous_cumulative
            previous_cumulative = current_cumulative

    # Build unified X-axis using quarter indices.
    steam_quarters = set(steam_agg.keys())
    ps_quarters = set(ps_agg.keys()) if has_ps else set()
    all_quarters = sorted(steam_quarters.union(ps_quarters))

    # Decide on quarterly vs yearly aggregation (if 12 or more quarters, aggregate yearly).
    use_yearly = len(all_quarters) >= 12
    if use_yearly:
        # Aggregate Steam data into yearly buckets.
        steam_yearly = {}
        for q, data in steam_agg.items():
            year = data["latest_date"].year
            if year not in steam_yearly:
                steam_yearly[year] = {
                    "quarterly_units": 0,
                    "quarterly_revenue": 0,
                    "cumulative_units": None,
                    "cumulative_revenue": None,
                    "max_q": -1,
                }
            steam_yearly[year]["quarterly_units"] += data["quarterly_units"]
            steam_yearly[year]["quarterly_revenue"] += data["quarterly_revenue"]
            if data["cumulative_units"] is not None and q > steam_yearly[year]["max_q"]:
                steam_yearly[year]["max_q"] = q
                steam_yearly[year]["cumulative_units"] = data["cumulative_units"]
                steam_yearly[year]["cumulative_revenue"] = data["cumulative_revenue"]

        if has_ps:
            ps_yearly = {}
            for q, data in ps_agg.items():
                year = data["latest_date"].year
                if year not in ps_yearly:
                    ps_yearly[year] = {
                        "quarterly_units": 0,
                        "cumulative_units": None,
                        "max_q": -1,
                    }
                ps_yearly[year]["quarterly_units"] += data["quarterly_units"]
                if data["cumulative_units"] is not None and q > ps_yearly[year]["max_q"]:
                    ps_yearly[year]["max_q"] = q
                    ps_yearly[year]["cumulative_units"] = data["cumulative_units"]

        # Build aggregated arrays using years.
        unified_years = sorted(set(steam_yearly.keys()).union(ps_yearly.keys() if has_ps else []))
        positions = np.arange(len(unified_years))
        aggregated_labels = [str(year) for year in unified_years]
        steam_cum_units = np.array([steam_yearly.get(year, {}).get("cumulative_units", np.nan) for year in unified_years], dtype=float) / 1e6
        steam_quarter_units = np.array([steam_yearly.get(year, {}).get("quarterly_units", np.nan) for year in unified_years], dtype=float) / 1e6
        steam_cum_revenue = np.array([steam_yearly.get(year, {}).get("cumulative_revenue", np.nan) for year in unified_years], dtype=float) / 1e6
        steam_quarter_revenue = np.array([steam_yearly.get(year, {}).get("quarterly_revenue", np.nan) for year in unified_years], dtype=float) / 1e6

        if has_ps:
            ps_cum_units = np.array([ps_yearly.get(year, {}).get("cumulative_units", np.nan) for year in unified_years], dtype=float) / 1e6
            ps_quarter_units = np.array([ps_yearly.get(year, {}).get("quarterly_units", np.nan) for year in unified_years], dtype=float) / 1e6
        else:
            ps_cum_units = None
            ps_quarter_units = None
    else:
        # Use quarterly data.
        positions = np.arange(len(all_quarters))
        aggregated_labels = [quarter_label(base_date, q) for q in all_quarters]
        steam_cum_units = np.array([steam_agg.get(q, {}).get("cumulative_units", np.nan) for q in all_quarters], dtype=float) / 1e6
        steam_quarter_units = np.array([steam_agg.get(q, {}).get("quarterly_units", np.nan) for q in all_quarters], dtype=float) / 1e6
        steam_cum_revenue = np.array([steam_agg.get(q, {}).get("cumulative_revenue", np.nan) for q in all_quarters], dtype=float) / 1e6
        steam_quarter_revenue = np.array([steam_agg.get(q, {}).get("quarterly_revenue", np.nan) for q in all_quarters], dtype=float) / 1e6

        if has_ps:
            ps_cum_units = np.array([ps_agg.get(q, {}).get("cumulative_units", np.nan) for q in all_quarters], dtype=float) / 1e6
            ps_quarter_units = np.array([ps_agg.get(q, {}).get("quarterly_units", np.nan) for q in all_quarters], dtype=float) / 1e6
        else:
            ps_cum_units = None
            ps_quarter_units = None

    # --- Compute Overall Totals from Steam Data ---
    if steam_agg:
        last_q = max(steam_agg.keys())
        total_units = steam_agg[last_q]["cumulative_units"]
        total_revenue = steam_agg[last_q]["cumulative_revenue"]
    else:
        total_units, total_revenue = 0, 0
    avg_unit_price = total_revenue / total_units if total_units else 0

    # Check if we have enough data points to plot.
    is_plotable = len(positions) >= 2

    return {
        "positions": positions,
        "aggregated_labels": aggregated_labels,
        "steam_cum_units": steam_cum_units,
        "steam_quarter_units": steam_quarter_units,
        "steam_cum_revenue": steam_cum_revenue,
        "steam_quarter_revenue": steam_quarter_revenue,
        "ps_cum_units": ps_cum_units,
        "ps_quarter_units": ps_quarter_units,
        "totals": {
            "total_units": total_units,
            "total_revenue": total_revenue,
            "avg_unit_price": avg_unit_price,
        },
        "is_plotable": is_plotable,
        "use_yearly": use_yearly,
    }



# ----------------------------------------------------------------------
# 3. Plotting
# ----------------------------------------------------------------------
def generate_sales_plot(aggregated_data, game_name, has_ps):
    """
    Generates a 2x2 grid plot using aggregated sales data.
    Returns a tuple: (image_stream, discord_file) where discord_file
    is a discord.File object ready for sending.
    """
    positions = aggregated_data["positions"]
    aggregated_labels = aggregated_data["aggregated_labels"]
    steam_cum_units = aggregated_data["steam_cum_units"]
    steam_quarter_units = aggregated_data["steam_quarter_units"]
    steam_cum_revenue = aggregated_data["steam_cum_revenue"]
    steam_quarter_revenue = aggregated_data["steam_quarter_revenue"]
    ps_cum_units = aggregated_data["ps_cum_units"]
    ps_quarter_units = aggregated_data["ps_quarter_units"]

    # Set up plotting parameters.
    rcParams.update({'font.size': 7})
    plt.rcParams['font.family'] = ['sans-serif']
    plt.rcParams['font.sans-serif'] = ['Arial', 'Helvetica', 'DejaVu Sans']

    # Create a 2x2 grid.
    fig, axs = plt.subplots(2, 2, figsize=(8, 4))

    # --- Left Column: Units Sold ---
    # Top left: Cumulative Units (Line Plot)
    ax_units_cum = axs[0, 0]
    ax_units_cum.plot(positions, steam_cum_units, marker='o', linestyle='-', color='#7289DA', markersize=3, label="Steam")
    if has_ps and ps_cum_units is not None:
        ax_units_cum.plot(positions, ps_cum_units, marker='o', linestyle='-', color='#ff6961', markersize=3, label="PlayStation")
    ax_units_cum.set_title(f"{game_name.upper()}, CUMULATIVE UNITS SOLD", fontsize=6, weight='bold', loc='left')
    ax_units_cum.set_xticks(positions)
    ax_units_cum.set_xticklabels(aggregated_labels, fontsize=6)
    ax_units_cum.yaxis.set_visible(False)
    for spine in ["top", "right", "bottom", "left"]:
        ax_units_cum.spines[spine].set_visible(False)
    for x, y in zip(positions, steam_cum_units):
        if np.isfinite(y):
            ax_units_cum.text(x, y + 0.03, format_value(y), fontsize=6, ha='center', va='bottom')
    if has_ps and ps_cum_units is not None:
        for x, y in zip(positions, ps_cum_units):
            if np.isfinite(y):
                ax_units_cum.text(x, y + 0.03, format_value(y), fontsize=6, ha='center', va='bottom', color='#ff6961')
        ax_units_cum.legend(loc="upper left", fontsize=6, frameon=False)

    # Bottom left: Quarterly/Yearly Units (Bar Chart)
    ax_units_quarter = axs[1, 0]
    bar_width = 0.3
    ax_units_quarter.bar(positions - bar_width/2, steam_quarter_units, width=bar_width, color='#7289DA', label="Steam")
    if has_ps and ps_quarter_units is not None:
        ax_units_quarter.bar(positions + bar_width/2, ps_quarter_units, width=bar_width, color='#ff6961', label="PlayStation")
    ax_units_quarter.set_title(f"{game_name.upper()}, UNITS SOLD", fontsize=6, weight='bold', loc='left')
    ax_units_quarter.set_xticks(positions)
    ax_units_quarter.set_xticklabels(aggregated_labels, fontsize=6)
    ax_units_quarter.yaxis.set_visible(False)
    for spine in ["top", "right", "bottom", "left"]:
        ax_units_quarter.spines[spine].set_visible(False)
    for x, y in zip(positions, steam_quarter_units):
        if np.isfinite(y):
            ax_units_quarter.text(x - bar_width/2, y + 0.03, format_value(y), fontsize=6, ha='center', va='bottom')
    if has_ps and ps_quarter_units is not None:
        for x, y in zip(positions, ps_quarter_units):
            if np.isfinite(y):
                ax_units_quarter.text(x + bar_width/2, y + 0.03, format_value(y), fontsize=6, ha='center', va='bottom', color='#ff6961')
        ax_units_quarter.legend(loc="upper left", fontsize=6, frameon=False)

    # --- Right Column: Revenue ---
    # Top right: Cumulative Revenue (Line Plot)
    ax_rev_cum = axs[0, 1]
    ax_rev_cum.plot(positions, steam_cum_revenue, marker='o', linestyle='-', color='#7289DA', markersize=3)
    ax_rev_cum.set_title(f"{game_name.upper()}, CUMULATIVE SALES", fontsize=6, weight='bold', loc='left')
    ax_rev_cum.set_xticks(positions)
    ax_rev_cum.set_xticklabels(aggregated_labels, fontsize=6)
    ax_rev_cum.yaxis.set_visible(False)
    for spine in ["top", "right", "bottom", "left"]:
        ax_rev_cum.spines[spine].set_visible(False)
    for x, y in zip(positions, steam_cum_revenue):
        if np.isfinite(y):
            ax_rev_cum.text(x, y + 0.03, format_revenue(y), fontsize=6, ha='center', va='bottom')

    # Bottom right: Quarterly/Yearly Revenue (Bar Chart)
    ax_rev_quarter = axs[1, 1]
    bars_rev = ax_rev_quarter.bar(positions, steam_quarter_revenue, color='#7289DA', width=0.6)
    ax_rev_quarter.set_title(f"{game_name.upper()}, SALES", fontsize=6, weight='bold', loc='left')
    ax_rev_quarter.set_xticks(positions)
    ax_rev_quarter.set_xticklabels(aggregated_labels, fontsize=6)
    ax_rev_quarter.yaxis.set_visible(False)
    for spine in ["top", "right", "bottom", "left"]:
        ax_rev_quarter.spines[spine].set_visible(False)
    for bar in bars_rev:
        height = bar.get_height()
        if np.isfinite(height):
            ax_rev_quarter.text(bar.get_x() + bar.get_width()/2, height + 0.03, format_revenue(height), fontsize=6, ha='center', va='bottom')

    plt.tight_layout()
    plt.show()
    image_stream = io.BytesIO()
    fig.savefig(image_stream, format='png')
    image_stream.seek(0)
    plt.close(fig)
    discord_file = discord.File(fp=image_stream, filename="plot.png")
    return image_stream, discord_file


# ----------------------------------------------------------------------
# 4. Discord Embed Creation
# ----------------------------------------------------------------------
def build_discord_embed(details, quick_data, totals, game_name, logos, similar_titles, plot_available):
    """
    Build and return a Discord embed with game details.
    """
    embed = Embed(
        title=details.get('name', game_name),
        description="Steam game details:",
        color=0x3498db,
        timestamp=datetime.datetime.now(pytz.utc)
    )
    rating = details.get("rating", 0)
    reviews = details.get("reviews", 0)
    embed.add_field(name="Rating", value=f"{rating:.2f}%", inline=True)
    embed.add_field(name="Reviews", value=format_reviews(reviews), inline=True)
    embed.add_field(name="Total Units", value=format_value(totals["total_units"]/1e6), inline=True)
    embed.add_field(name="Total Revenue", value=format_revenue(totals["total_revenue"]/1e6), inline=True)
    embed.add_field(name="Avg Unit Price", value=f"${totals['avg_unit_price']:.2f}", inline=True)

    max_players_24h = quick_data.get("max_players_24h")
    players_latest = quick_data.get("players_latest")
    players_latest_time = quick_data.get("players_latest_time")
    if max_players_24h is not None and max_players_24h != -1:
        embed.add_field(name=f"Current Players ({players_latest_time} min ago)", value=str(players_latest))
        embed.add_field(name="Max Players (24h)", value=str(max_players_24h), inline=True)

    # Add PlayStation details if present.
    if "units_sold_ps" in details and details["units_sold_ps"] is not None:
        embed.add_field(name="Playstation details:", value=f"Total units PS: {format_value(details['units_sold_ps']/1e6)}", inline=False)
    
    if similar_titles:
        embed.add_field(name="Games with similar titles:", value="\n".join(similar_titles), inline=False)
    
    if not plot_available:
        embed.add_field(name="Graph", value="Not enough data to generate a graph.", inline=False)
    
    embed.set_footer(text="Data source: vginsights.com")
    if logos:
        capsule_url = logos.get("steam", {}).get("capsule")
        if capsule_url:
            embed.set_thumbnail(url=capsule_url)
    return embed


# ----------------------------------------------------------------------
# Main Function (Coordinator)
# ----------------------------------------------------------------------
def steam(game_name):
    """
    Main coordinator function that:
      1. Retrieves raw data.
      2. Aggregates and orders the data.
      3. Generates plots (if enough data is available).
      4. Builds the final Discord embed.
    Returns a dictionary with the plot, embed, and aggregated stats.
    """
    # 1. Data Retrieval
    raw_data = retrieve_game_data(game_name)

    # 2. Data Aggregation
    aggregated_data = aggregate_sales_data(raw_data)

    # 3. Plotting
    if aggregated_data["is_plotable"]:
        image_stream, discord_file = generate_sales_plot(aggregated_data, raw_data["game_name"], raw_data["has_ps"])
    else:
        image_stream, discord_file = None, None

    # 4. Discord Embed Creation
    embed = build_discord_embed(
        details=raw_data["details"],
        quick_data=raw_data["quick_data"],
        totals=aggregated_data["totals"],
        game_name=raw_data["game_name"],
        logos=raw_data["logos"],
        similar_titles=raw_data["similar_titles"],
        plot_available=bool(image_stream)
    )

    return {
        "image_stream": image_stream,
        "discord_file": discord_file,
        "embed": embed,
        "details": raw_data["details"],
        "sales": raw_data["sales_data"],
        "aggregated": aggregated_data["totals"],
    }


# ----------------------------------------------------------------------
# Discord Command Wrapper (for async bots)
# ----------------------------------------------------------------------
async def steam_command(ctx, *, game_name):
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, steam, game_name)
    except Exception as e:
        await ctx.send("Failed to generate steam data, check game name.")
        return
    await ctx.send(embed=result["embed"])
    if result["discord_file"] is not None:
        await ctx.send(file=result["discord_file"])


# ----------------------------------------------------------------------
# Example Usage (for testing purposes)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    try:
        result = steam("kingdom come deliverance")
        print("Steam data retrieved successfully.")
    except Exception as err:
        print(f"Error: {err}")
