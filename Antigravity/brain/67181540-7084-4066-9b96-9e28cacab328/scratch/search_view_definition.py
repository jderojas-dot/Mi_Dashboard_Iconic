import os

app_data_dir = r"C:\Users\JAVIER\.gemini\antigravity\brain\bf393c7c-0d72-446a-b80b-2e424a663ddf"
if os.path.exists(app_data_dir):
    for root, dirs, files in os.walk(app_data_dir):
        for file in files:
            rel = os.path.relpath(os.path.join(root, file), app_data_dir)
            print(rel)
else:
    print("Directory does not exist")
