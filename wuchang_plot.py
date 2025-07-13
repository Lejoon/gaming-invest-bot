import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from database import Database
from general_utils import generate_gts_placements_plot_with_minmax
import io

def create_wuchang_plot():
    """
    Generates a plot for "Wuchang: Fallen Feathers" with min, max, and average placements.
    """
    db_name = "steam_top_games.db"
    game_name = "Wuchang: Fallen Feathers"
    output_filename = "wuchang_placements.png"

    with Database(db_name) as db:
        # A new function to get min/max/avg data
        aggregated_data = db.get_gts_placements_with_minmax(game_name)

    if aggregated_data:
        # Generate the original plot with min/max/avg data
        plot_buffer = generate_gts_placements_plot_with_minmax(aggregated_data, game_name)
        
        if plot_buffer:
            with open(output_filename, 'wb') as f:
                f.write(plot_buffer.getvalue())
            print(f"Plot saved to {output_filename}")
        else:
            print("Failed to generate plot.")

    else:
        print(f"No data found for {game_name}.")

def create_wuchang_plot_delta_days():
    """
    Generates a comparison plot between two games with x-axis as delta days to release.
    Change the variables below to customize the comparison game.
    """
    # Configuration variables - change these as needed
    primary_game_name = "Wuchang: Fallen Feathers"
    primary_release_date = "2025-07-24"
    
    comparison_game_name = "Stellar Blade™"  # Change this to any game name
    comparison_release_date = "2024-04-26"   # Change this to the game's release date (YYYY-MM-DD)
    
    db_name = "steam_top_games.db"
    output_filename = "wuchang_placements_delta_days.png"
    days_before_release = 30  # Variable days to look back
    
    from general_utils import generate_comparison_placements_plot_delta_days
    
    with Database(db_name) as db:
        # Get data for primary game
        primary_data = db.get_game_placements_delta_days(
            primary_game_name, 
            primary_release_date, 
            days_before_release
        )
        
        # Get data for comparison game
        comparison_data = db.get_game_placements_delta_days(
            comparison_game_name, 
            comparison_release_date, 
            days_before_release
        )
        
        # Combine the data
        games_data = {}
        if primary_data:
            games_data[primary_game_name] = primary_data
            print(f"Found data for {primary_game_name}: {len(primary_data['delta_days'])} data points")
        else:
            print(f"No data found for {primary_game_name}")
            
        if comparison_data:
            games_data[comparison_game_name] = comparison_data
            print(f"Found data for {comparison_game_name}: {len(comparison_data['delta_days'])} data points")
        else:
            print(f"No data found for {comparison_game_name}")

    if len(games_data) >= 2:
        plot_buffer = generate_comparison_placements_plot_delta_days(
            games_data, 
            primary_game_name, 
            comparison_game_name,
            days_before_release
        )
        if plot_buffer:
            with open(output_filename, 'wb') as f:
                f.write(plot_buffer.getvalue())
            print(f"Comparison delta days plot saved to {output_filename}")
            print(f"Comparing: {primary_game_name} vs {comparison_game_name}")
        else:
            print("Failed to generate comparison delta days plot.")
    elif len(games_data) == 1:
        print(f"Only found data for one game: {list(games_data.keys())[0]}")
    else:
        print("No data found for either game.")

if __name__ == "__main__":
    create_wuchang_plot()
    create_wuchang_plot_delta_days()
