import glob
import pandas as pd
import os

def merge_csv(source_dir, output_file):
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    files = glob.glob(os.path.join(source_dir, "*.csv"))
    if not files:
        print("Aucun fichier à merger.")
        return
    df_list = []
    for f in files:
        try:
            df = pd.read_csv(f, header=None)
            if not df.empty:
                df_list.append(df)
        except pd.errors.EmptyDataError:
            print(f"Fichier vide ignoré : {f}")
    if not df_list:
        print("Aucune donnée à merger (tous les fichiers sont vides).")
        return
    merged = pd.concat(df_list)
    merged.to_csv(output_file, index=False, header=False)
    print(f"Fichiers mergés dans {output_file}")

if __name__ == "__main__":
    merge_csv("output/connections_per_ip", "output/merge/connections_per_ip_merged.csv")
    merge_csv("output/connections_per_agent", "output/merge/connections_per_agent_merged.csv")
    merge_csv("output/connections_per_day", "output/merge/connections_per_day_merged.csv")