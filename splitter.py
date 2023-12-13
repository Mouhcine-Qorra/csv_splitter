import os
import pandas as pd
import uuid


class FileSettings(object):
    def __init__(self, file_name, row_size=100):
        self.file_name = file_name
        self.row_size = row_size


class FileSplitter(object):

    def __init__(self, file_settings):
        self.file_settings = file_settings

        if type(self.file_settings).__name__ != "FileSettings":
            raise Exception("Please pass the correct instance ")

        self.file_header = pd.read_csv(self.file_settings.file_name, nrows=4, header=None)
        self.df = pd.read_csv(self.file_settings.file_name, skiprows=3, chunksize=self.file_settings.row_size)

    def run(self, directory="temp"):

        try:
            os.makedirs(directory)
        except Exception as e:
            pass

        counter = 0

        while True:
            try:
                file_name = "{}/{}_row_{}_{}.csv".format(
                    directory, self.file_settings.file_name.split(".")[0], counter, uuid.uuid4().__str__()
                )
                self.file_header.to_csv(file_name, header=False, index=False)
                df_chunk = next(self.df)
                df_chunk.to_csv(file_name, header=False, index=False, mode='a')  # Append without header and index
                counter += 1
            except StopIteration:
                break
            except Exception as e:
                print("Error:", e)
                break

        return True


def main():
    helper = FileSplitter(FileSettings(
        file_name='Load-test-data-Valid-fields.csv',
        row_size=240
    ))
    helper.run()


main()
