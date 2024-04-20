import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
	raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Any, Iterable
import pandas as pd

from fileStreams import getFileJsonStream


fileOrFolderPath = r"/Users/kevinwang/Desktop/Projects/AI art/Reddit/reddit/comments/RC_2024-03.zst_blocks"
recursive = False
ID_file_Doctor_Who = pd.read_csv("Doctor_Who_submission.csv", header=None)
ID_file_Doctor_Who = ID_file_Doctor_Who[0].tolist()

ID_file_Wizards = pd.read_csv("Wizards_submission.csv", header=None)
ID_file_Wizards = ID_file_Wizards[0].tolist()

ID_file_Lego = pd.read_csv("Lego_submission.csv", header=None)
ID_file_Lego = ID_file_Lego[0].tolist()

ID_file_Hasbro = pd.read_csv("Hasbro_submission.csv", header=None)
ID_file_Hasbro = ID_file_Hasbro[0].tolist()

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
	parent_id = row["parent_id"]
	parent_id = parent_id.split("_")[1]
	
	if parent_id in ID_file_Doctor_Who:
		print(parent_id)
		with open("Doctor_Who_comments.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["parent_id"]) + "," + escape_csv_field(row["body"]) + "\n")
	if parent_id in ID_file_Wizards:
		print(parent_id)
		with open("Wizards_comments.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["parent_id"]) + "," + escape_csv_field(row["body"]) + "\n")
	if parent_id in ID_file_Lego:
		print(parent_id)
		with open("Lego_comments.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["parent_id"]) + "," + escape_csv_field(row["body"]) + "\n")
	if parent_id in ID_file_Hasbro:
		print(parent_id)
		with open("Hasbro_comments.csv", "a") as f:
			f.write(escape_csv_field(row["id"]) + "," + escape_csv_field(row["parent_id"]) + "," + escape_csv_field(row["body"]) + "\n")

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
