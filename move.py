import os
import shutil

def move_docx_files_to_main_directory(main_directory):
    for root, _, files in os.walk(main_directory):
        for file in files:
            if file.endswith('.docx'):
                source = os.path.join(root, file)
                destination = os.path.join(main_directory, file)
                if source != destination:
                    print(f'Moving {source} to {destination}')
                    shutil.move(source, destination)

if __name__ == '__main__':
    main_directory = './hspolicy22'
    move_docx_files_to_main_directory(main_directory)