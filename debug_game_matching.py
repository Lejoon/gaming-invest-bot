\
import sqlite3
import difflib
from general_utils import normalize_game_name_for_search

DB_NAME = "steam_top_games.db"
USER_QUERY_TO_TEST = "kingdom come deliverance 2"

def debug_get_best_game_match(user_query_raw: str, db_path: str):
    """
    Debugs the game matching logic by showing intermediate steps and results.
    """
    print(f"Debugging game matching for: '{user_query_raw}'\\n")

    conn = None
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT game_name FROM GameTranslation")
        original_game_names = [row[0] for row in cursor.fetchall()]
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        if conn:
            conn.close()

    if not original_game_names:
        print("No game names found in GameTranslation table.")
        return None

    normalized_query = normalize_game_name_for_search(user_query_raw)
    print(f"Normalized user query: '{normalized_query}'\\n")

    if not normalized_query:
        print("Normalized user query is empty.")
        return None

    # Create pairs of (normalized_name, original_name) from the database
    db_name_pairs = []
    print("First 10 original and normalized game names from DB for reference:")
    for i, orig_name in enumerate(original_game_names):
        norm_name = normalize_game_name_for_search(orig_name)
        if norm_name:
            db_name_pairs.append((norm_name, orig_name))
            if i < 10:
                print(f"  Original: '{orig_name}' -> Normalized: '{norm_name}'")
    if len(original_game_names) > 10:
        print("  ... and more ...")
    print("\\n")


    # --- Stage 1: Word-level match ---
    print("--- Stage 1: Word-level match ---")
    query_tokens = normalized_query.split()
    word_matches = [
        (norm, orig) for norm, orig in db_name_pairs
        if all(token in norm.split() for token in query_tokens)
    ]

    if word_matches:
        print(f"Found {len(word_matches)} word-level match(es):")
        for norm, orig in word_matches[:10]: # Print first 10 matches
             print(f"  Normalized: '{norm}', Original: '{orig}'")
        if len(word_matches) > 10:
            print("  ... and more ...")
        
        word_matches.sort(key=lambda x: len(x[0])) # Sort by length of normalized name
        chosen_match = word_matches[0][1]
        print(f"Chosen word-level match (shortest normalized): '{chosen_match}' (Normalized: '{word_matches[0][0]}')")
        return chosen_match
    else:
        print("No word-level matches found.")

    # --- Stage 2: Prefix match ---
    print("\\n--- Stage 2: Prefix match ---")
    prefix_matches = [
        (norm, orig) for norm, orig in db_name_pairs if norm.startswith(normalized_query)
    ]
    if prefix_matches:
        print(f"Found {len(prefix_matches)} prefix match(es):")
        for norm, orig in prefix_matches[:10]:
            print(f"  Normalized: '{norm}', Original: '{orig}'")
        if len(prefix_matches) > 10:
            print("  ... and more ...")

        chosen_match_pair = min(prefix_matches, key=lambda x: len(x[0])) # Shortest normalized prefix match
        chosen_match = chosen_match_pair[1]
        print(f"Chosen prefix match (shortest normalized): '{chosen_match}' (Normalized: '{chosen_match_pair[0]}')")
        return chosen_match
    else:
        print("No prefix matches found.")

    # --- Stage 3: Substring match ---
    print("\\n--- Stage 3: Substring match ---")
    substring_matches = [
        (norm, orig) for norm, orig in db_name_pairs if normalized_query in norm
    ]
    if substring_matches:
        print(f"Found {len(substring_matches)} substring match(es):")
        for norm, orig in substring_matches[:10]:
            print(f"  Normalized: '{norm}', Original: '{orig}'")
        if len(substring_matches) > 10:
            print("  ... and more ...")
            
        chosen_match_pair = min(substring_matches, key=lambda x: len(x[0])) # Shortest normalized substring match
        chosen_match = chosen_match_pair[1]
        print(f"Chosen substring match (shortest normalized): '{chosen_match}' (Normalized: '{chosen_match_pair[0]}')")
        return chosen_match
    else:
        print("No substring matches found.")

    # --- Stage 4: Difflib fallback ---
    print("\\n--- Stage 4: Difflib fallback ---")
    normalized_db_names_for_difflib = [p[0] for p in db_name_pairs]
    close_matches_normalized = difflib.get_close_matches(normalized_query, normalized_db_names_for_difflib, n=1, cutoff=0.75)
    
    if close_matches_normalized:
        print(f"Difflib found close normalized match(es): {close_matches_normalized}")
        # Find the original name corresponding to the normalized close match
        chosen_normalized_name = close_matches_normalized[0]
        chosen_match = next((orig for norm, orig in db_name_pairs if norm == chosen_normalized_name), None)
        if chosen_match:
            print(f"Chosen difflib match: '{chosen_match}' (Normalized: '{chosen_normalized_name}')")
            return chosen_match
        else:
            print(f"Could not find original name for difflib match '{chosen_normalized_name}'. This shouldn't happen.")
            return None
    else:
        print("No difflib matches found (cutoff 0.75).")

    print("\\nNo match found by any method.")
    return None

if __name__ == "__main__":
    print(f"Starting game matching debugger for query: '{USER_QUERY_TO_TEST}'")
    print(f"Using database: {DB_NAME}\\n")
    
    final_match = debug_get_best_game_match(USER_QUERY_TO_TEST, DB_NAME)
    
    if final_match:
        print(f"\\n----------------------------------------------------")
        print(f"Overall best match for '{USER_QUERY_TO_TEST}': '{final_match}'")
        print(f"----------------------------------------------------")
    else:
        print(f"\\n----------------------------------------------------")
        print(f"No overall match found for '{USER_QUERY_TO_TEST}'.")
        print(f"----------------------------------------------------")

    # You can also test the original get_best_game_match from steam.py if you want to compare
    # from steam import get_best_game_match # Assuming Database class is handled or mocked
    # class DummyDB:
    #   def __init__(self, path): self.conn = sqlite3.connect(path)
    # db_instance = DummyDB(DB_NAME)
    # original_function_match = get_best_game_match(USER_QUERY_TO_TEST, db_instance)
    # print(f"Result from original get_best_game_match: '{original_function_match}'")
    # db_instance.conn.close()
