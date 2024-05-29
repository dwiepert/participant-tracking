"""
Run participant tracking manually

Author(s): Daniela Wiepert
Last modified: 05/28/2024
"""

### IMPORTS ###
#built-in
import argparse
import ast
import os
import subprocess
import time
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--py_path", default='')
    parser.add_argument("--src_path", default="./uploaded_sheets")
    parser.add_argument("--input_path", default="./uploaded_sheets")
    parser.add_argument("--input_archive", default="./uploaded_sheets/archive")
    parser.add_argument("--output_path", default="./output_sheets")
    parser.add_argument("--output_archive",default="./output_sheets/archive")
    parser.add_argument("--to_filter", default="filter.csv")
    parser.add_argument("--qualtrics_mrn", default='./code/qualtrics_mrn/qualtrics_mrn.csv')
    parser.add_argument("--decrypt_code", default="0726")
    parser.add_argument("--load_existing", type=ast.literal_eval, default=True)
    parser.add_argument("--browser", default="chrome")
    parser.add_argument("--headless", type=ast.literal_eval, default=True)
    parser.add_argument("--testing_mode", type=ast.literal_eval, default=False)
    args = parser.parse_args()

    py_path = Path(__file__).absolute().parents[0]
    py_path_watch = py_path.parents[0]
    os.chdir(py_path_watch)

    args.src_path = Path(args.src_path).absolute()
    args.input_path = Path(args.input_path).absolute()
    args.input_archive = Path(args.input_archive).absolute()
    args.output_path = Path(args.output_path).absolute()
    args.output_archive = Path(args.output_archive).absolute()
    args.to_filter = Path(args.to_filter).absolute()
    args.qualtrics_mrn = Path(args.qualtrics_mrn).absolute()

    combine_args = ["python", str(py_path / "combine_sheets_v5.py"),
                    f"--py_path={args.py_path}", 
                    f"--input_path={args.input_path}", f"--input_archive={args.input_archive}",
                    f"--output_path={args.output_path}", f"--output_archive={args.output_archive}",
                    f"--to_filter={args.to_filter}", f"--qualtrics_mrn={args.qualtrics_mrn}",
                    f"--load_existing={args.load_existing}", f"--decrypt_code={args.decrypt_code}"]
    
    automate_args = ["python",str(py_path /"automated_qualtrics_v4.py"),
                    f"--py_path={args.py_path}", f"--output_path={args.output_path}",
                    f"--browser={args.browser}", f"--headless={args.headless}", f"--testing_mode={args.testing_mode}"]
    
    archive_args =  ["python", str(py_path / "archive_v1.py"),
                    f"--py_path={args.py_path}", 
                    f"--input_path={args.input_path}", f"--input_archive={args.input_archive}",
                    f"--output_path={args.output_path}", f"--output_archive={args.output_archive}"]
    
    subprocess.run(combine_args)
    results = subprocess.run(automate_args, capture_output=True, text=True)
    print(results.stdout)
    print(results.stderr)
    if not 'No participants in NewInterested.' in results.stdout:
        subprocess.run(combine_args)

    subprocess.run(archive_args)
    print('Combine completed.')

if __name__ == '__main__':
    main()