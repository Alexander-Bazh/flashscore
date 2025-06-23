import sys
from flashscore import poisk_kef

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} MATCH_ID SPORT_ID")
        sys.exit(1)

    match_id = sys.argv[1]
    try:
        sport_id = int(sys.argv[2])
    except ValueError:
        print("SPORT_ID must be an integer")
        sys.exit(1)

    kefs, kefs_dc, kefs_ou, kefs_bts = poisk_kef(match_id, sport_id)

    print("1X2:", kefs)
    print("Double Chance:", kefs_dc)
    print("Over/Under:", kefs_ou)
    print("Both Teams To Score:", kefs_bts)
