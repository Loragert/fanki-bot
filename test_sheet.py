import gspread

gc = gspread.oauth(credentials_filename="credentials.json")
sh = gc.open("Bot Tasks")
worksheet = sh.sheet1

worksheet.append_row(["Test User ID", "TestUsername", "Test Task", "Done"])
print("Запис додано!")