import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
	raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Any, Iterable

from fileStreams import getFileJsonStream


fileOrFolderPath = r"/Users/kevinwang/Desktop/Projects/AI art/Reddit/reddit/submissions/RS_2024-03.zst_blocks"
recursive = False

def escape_csv_field(field):
    if field:
        # 替换内部的双引号
        field = field.replace('"', '""')
        # 为包含逗号、双引号、换行符的字段添加双引号
        if ',' in field or '"' in field or '\n' in field:
            field = '"' + field + '"'
    else:
        field = '""'
    return field

def processRow(row: dict[str, Any]):
	if "Doctor Who" in row.get("title", ""):
		# 将 id, title和具体内容 写入一个csv文件，注意不要覆盖文件之前的内容
		with open("Doctor_Who_submission.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["title"]) + "," + escape_csv_field(row["selftext"]) + "\n")
	if "Lego" in row.get("title", "") or "LEGO" in row.get("title", ""):
		# 将 id 写入一个csv文件，注意不要覆盖文件之前  的内容
		with open("Lego_submission.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["title"]) + "," + escape_csv_field(row["selftext"]) + "\n")
	if "Wizards" in row.get("title", "") or "wizards" in row.get("title", ""):
		# 将 id 写入一个csv文件，注意不要覆盖文件之前的内容
		with open("Wizards_submission.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["title"]) + "," + escape_csv_field(row["selftext"]) + "\n")
	if "Hasbro" in row.get("title", "") or "hasbro" in row.get("title", ""):
		# 将 id 写入一个csv文件，注意不要覆盖文件之前的内容
		with open("Hasbro_submission.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["title"]) + "," + escape_csv_field(row["selftext"]) + "\n")

def processFile(path: str):
	jsonStream = getFileJsonStream(path)
	if jsonStream is None:
		print(f"Skipping unknown file {path}")
		return
	for i, (lineLength, row) in enumerate(jsonStream):
		if i % 10_000 == 0:
			print(f"\rRow {i}", end="")
		processRow(row)
	print(f"\rRow {i+1}")

def processFolder(path: str):
	fileIterator: Iterable[str]
	if recursive:
		def recursiveFileIterator():
			for root, dirs, files in os.walk(path):
				for file in files:
					yield os.path.join(root, file)
		fileIterator = recursiveFileIterator()
	else:
		fileIterator = os.listdir(path)
		fileIterator = (os.path.join(path, file) for file in fileIterator)
	
	for i, file in enumerate(fileIterator):
		print(f"Processing file {i+1: 3} {file}")
		processFile(file)

def main():
	if os.path.isdir(fileOrFolderPath):
		processFolder(fileOrFolderPath)
	else:
		processFile(fileOrFolderPath)
	
	print("Done :>")

if __name__ == "__main__":
	main()
