import time
import os
import datetime
import logging

import pandas as pd
from cv2 import imread, imwrite, IMWRITE_JPEG_QUALITY


""" CONSTANTS """
yadisk_dir = "/mnt/c/Users/zer0nu11/YandexDisk-diwwa.e/qweqwe"
prod_server_dir = "/mnt/c/Users/zer0nu11/Documents/workspace/sreda/res"
logs_dir = "logs"
reports_dir = "reports"


class Record:
    """
        Dir name    - имя папки с я.диска
        Path        - путь до папки включаяя имя
        Exist files - список картинок из прод-папки, название которых совподает с папкой я.диск
        Added files - список картинок, которые скопировали из этой папки в прод-папку
        Wrong files - список картинок, код которых не совпадает с названием папки, в которой они лежат
        Outsiders   - сообщения про то, что в папке есть картинки, с другим кодом
        Duplicates  - сообщения про то, что есть несколько папок с одним кодом в названии
        Comment     - нет фото подходящих фото \ есть только главное фото \ есть только доп. фото \ есть оба вида фото
        Statistics  - число фотографий в продакшене после скрипта (скопировано / всего)
    """
    def __init__(self, dir_name, path, exist_files, added_files, wrong_files, outsiders, duplicates, comment, stats):
        self.dir_name = dir_name
        self.path = path
        self.exist_files = exist_files
        self.added_files = added_files
        self.wrong_files = wrong_files
        self.outsiders = outsiders
        self.duplicates = duplicates
        self.comment = comment
        self.stats = stats
        
    def getList(self) -> list:
        return [self.dir_name, self.path, self.exist_files, self.added_files, 
                self.wrong_files, self.duplicates, self.comment, self.stats]

class Reporter:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.table_columns = ["Dir name","Path","Exist files","Added files","Wrong files",
                                "Outsiders","Duplicates","Comment","Statistics"]
        self.report_table = pd.DataFrame(columns=self.table_columns)
    
    def addRecord(self, record : Record):
        tmp_df = pd.DataFrame(data=[record.getList()], columns=self.table_columns)
        self.report_table = pd.concat([self.report_table,tmp_df], ignore_index=True)

    def _save2csv(self):
        self.report_table.to_csv(f"{self.path}/{self.name}.csv", index=False)

    def _save2xls(self):
        self.report_table.to_excel(f"{self.path}/{self.name}.xlsx", index=False)

    def save_log(self):
        self._save2csv()
        self._save2xls()

# ========================================================================
class Image:
    def __init__(self, name: str, path: str, datetime: datetime.datetime):
        self.name = name
        self.path = path
        self.datetime = datetime

    def __str__(self):
        return f"{self.name} : {self.path} : {self.datetime}"

    def __repr__(self):
        return f"{self.name}"
    
    def __eq__(self, other):
        if not isinstance(other, Image):
            logging.error(f"Trying to compare non-Image: {other}")
            raise TypeError("Trying to compare non-Image : Image.__eq__()")
        name1 = self.name[:self.name.rfind('.')].replace('-','_')
        name2 = other.name[:other.name.rfind('.')].replace('-','_')
        return name1 == name2

class Checker:
    """ Checker for files """
    def __init__(self, checking_path, output_queue, reporter, input_queue=None):
        self.checking_path = checking_path
        self.input_queue = input_queue
        self.output_queue = output_queue
        self.reporter = reporter
        self.supported_formats = ['jpeg','jpg','png','webp']
        self.checking_queue = self.make_checking_queue(checking_path)
        # self.checking_table = self.make_table()
        logging.info(f'Found {len(self.checking_queue)} images in [{checking_path}]')
    
    def make_checking_queue(self, checking_path):
        """ Make queue of Images() for checking """
        tmp_queue = []
        for root, _, filenames in os.walk(checking_path):
            for filename in filenames:
                if filename[filename.rfind('.')+1:] in self.supported_formats:
                    tmp_queue.append(
                        Image(
                            filename,
                            root,
                            datetime.datetime.fromtimestamp(os.path.getctime(root+'/'+filename))
                        )
                    )
        return tmp_queue

    def make_table(self):
        """ Create table from images in checking_path and nested folders """
        tmp_dict = {'name':[],'path':[],'ctime':[]}
        for image in self.checking_queue:
            tmp_dict['name'].append(image.name)
            tmp_dict['path'].append(image.path)
            tmp_dict['ctime'].append(image.datetime)
        return pd.DataFrame(tmp_dict)

    def check(self, image: Image):
        """ Specific checking algorithm for image """
        return

    def run(self):
        """ Run checking routine """
        return

class diskChecker(Checker):
    """ Check images on ya.disk """

    def _check_mask(self, image: Image): # TODO WTF
        """ Check name for mask """
        
        RULES = {'0': lambda c:c.isdigit(), '_': lambda c:c=='_' or c=='-', '.': lambda c:c=='.', 'a': lambda c:c.isalpha()}
        mask_one = '00000.a'
        mask_two = '00000_0.a'
        for mask in [mask_one, mask_two]:
            if len(image.name) > len(mask): mask += 'a'*(len(image.name)-len(mask))
            elif len(image.name) < len(mask): return False
            for index, rule in enumerate(mask):
                if not RULES[rule](image.name[index]):
                    return False
            return True

    def _check_duplicates(self, image: Image) -> int:
        copies = 0
        newest_image = None
        for exist_image in self.checking_queue:
            if exist_image == image:
                copies += 1
                if newest_image:
                    newest_image = exist_image
                elif exist_image.datetime > newest_image.datetime:
                    newest_image = exist_image
        self._tmp = newest_image
        return copies

    def check(self, image: Image):
        if self._check_mask(image):
            copies_num = self._check_duplicates(image)
            if copies_num > 1:
                # TODO excel report (Reporter() class)
                pass
            return True
        return False

    def run(self):
        """ Iterator over input queue with filtering via check() and writing to output queue """
        logging.info(f'Starting ya.disk check')
        self._tmp = None
        for image in self.checking_queue:
            if self.check(image):
                self.output_queue.append(self._tmp)
        logging.info('Finishing ya.disk check')

class prodChecker(Checker):
    """ Check production images """

    def _image_exist(self, image: Image):
        """ return existing image from folder or None if it's not existing """
        for exist_image in self.checking_queue:
            if exist_image == image:
                return exist_image
        return None

    def _image_newer(self, image1: Image, image2: Image):
        """ return True if image1 is newer than image2 """
        return image1.datetime > image2.datetime

    def check(self, image: Image):
        exist_image = self._image_exist(image)
        if exist_image:
            if self._image_newer(image, exist_image): 
                return True
            else:
                return False
        return True
    
    def run(self):
        """ Iterator over input queue with filtering via check() and writing to output queue """
        logging.info(f'Starting prod-folder check')
        for image in self.input_queue:
            if self.check(image):
                self.output_queue.append(image)
        logging.info('Finishing prod-folder check')

class Converter:
    """ Convert image """
    def __init__(self, format='jpg', quality=100):
        self.format = format
        self.quality = quality

    def _compress_image(self, image: Image, save_path):
        os.system('jpegoptim -ptm85 ' + save_path.replace(' ','\ ') + '/' + image.name)

    def _convert_to_jpg(self, image: Image):
        read_path = image.path + '/' + image.name
        new_name = image.name[:image.name.rfind('.')+1] + self.format
        write_path = image.path + '/' + new_name.replace('-','_')
        image.name = new_name

        if os.path.exists(read_path):
            tmp_img = imread(read_path)
            imwrite(write_path, tmp_img, [int(IMWRITE_JPEG_QUALITY), 100])
        else:
            logging.error(f"Image {read_path} disappeared")

    def convert_image(self, image: Image, save_path: str):
        img_format = image.name[image.name.rfind(".")+1:]
        if img_format != self.format:
            self._convert_to_jpg(image)
        self._compress_image(image, save_path)
    
class Mover:
    """ Convert and move images to prod """
    def __init__(self, input_queue, destination_path, reporter):
        self.input_queue = input_queue
        self.destination_path = destination_path
        self.reporter = reporter
        self.converter = Converter()

    def run(self):
        logging.info(f'Trying to convert and move {len(self.input_queue)} objects')
        counter = 0
        for image in self.input_queue:
            self.converter.convert_image(image, self.destination_path)
            counter += 1
        logging.info(f'{counter} objects converted and moved to prod-folder')
# ========================================================================

if __name__ == "__main__":
    if not os.path.exists(logs_dir):
        os.mkdir(logs_dir)
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)
        
    dt = datetime.datetime.now().strftime('%d.%m.%Y %H-%M')
    logging.basicConfig(level=logging.INFO, filename=f"{logs_dir}/sreda-{dt}.log", filemode="w",
                format="%(asctime)s %(levelname)s %(message)s")
    logging.info(f"Starting")

    reporter = Reporter("report", reports_dir)

    # ========================================================================
    
    # Queues for transfering images between handlers
    check_wishlist = []
    copy_wishlist = []

    
    diskchecker = diskChecker(checking_path=yadisk_dir, 
                              output_queue=check_wishlist, reporter=reporter)
    prodchecker = prodChecker(checking_path=prod_server_dir, 
                              input_queue=check_wishlist, output_queue=copy_wishlist, reporter=reporter)
    filemover = Mover(input_queue=copy_wishlist, destination_path=prod_server_dir, reporter=reporter) 
    logging.info(f"Initialization over")
    
    tmp_time = time.time()
    diskchecker.run()
    logging.info(f"Checking ya.disk takes {int(time.time()-tmp_time)} sec")

    tmp_time = time.time()
    prodchecker.run()
    logging.info(f"Checking prod-folder takes {int(time.time()-tmp_time)} sec")
    
    tmp_time = time.time()
    filemover.run()
    logging.info(f"Moving images takes {int(time.time()-tmp_time)} sec")
    # ========================================================================

    reporter.save_log()