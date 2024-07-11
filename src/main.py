import time
import os
import datetime
import logging
import re
import collections

from tqdm import tqdm
import pandas as pd
from PIL import Image as pil

""" CONSTANTS """
yadisk_dir = "/mnt/c/Users/zer0nu11/Desktop/Work/Images_test"
prod_server_dir = "/mnt/c/Users/zer0nu11/Desktop/Work/out"
logs_dir = "logs"
reports_dir = "reports"

supported_extensions = ['jpeg','jpg','png','webp']
supported_extensions = supported_extensions + [e.upper() for e in supported_extensions]


# ========================= DATATYPES =========================
class Image:
    def __init__(self, filename: str, code: int, number: int, \
                    extension: str, ctime: datetime.datetime, weight: int, path: str, shape: list[int]):
        self.filename = filename
        self.code = code
        self.number = number
        self.extension = extension
        self.ctime = ctime
        self.weight = weight
        self.path = path
        self.shape = shape # format: (width, height)
        self.newest = False # newest of ya.disk
        self.onprod = False # photo with same code and number already on prod
        self.latest = False # latest of all photos, including prod
        self.moved = False # if photo moved to prod
        self.wrong_dir = False # if photo location in wrong directory

    def __str__(self):
        return '{0:<35} {1}'.format(self.filename, self.path)
    def __repr__(self):
        return f"{self.filename}"
    def __eq__(self, other):
        if not isinstance(other, Image):
            logging.error(f"Trying to compare non-Image: {other}")
            raise TypeError("Trying to compare non-Image : Image.__eq__()")
        return (self.code == other.code) and (self.number == other.number)

class Folder:
    def __init__(self, foldername: str, code: int, path: str, files: list[Image]):
        self.foldername = foldername
        self.code = code
        self.path = path
        self.files = files
        self.clones = []
        self.prodfiles = []

    def __str__(self):
        return '{0:<35} {1}'.format(self.foldername, self.path)
    def __repr__(self):
        return f"{self.foldername}"

# ========================== CLASSES ==========================
class ImageHandler:
    def __init__(self):
        self.codeSearcher = re.compile('^\d{5}')
        self.numberSearcher = re.compile('([-_]\d+)?\.')
        self.extensionSearcher = re.compile('\.(?:{})$'.format('|'.join(supported_extensions)))
    
    def createImage(self, filename: str, path: str) -> Image:
        code = int(self.codeSearcher.findall(filename)[0])
        number = [int(tmp[1:]) if tmp else None for tmp in self.numberSearcher.findall(filename)][0]
        extension = self.extensionSearcher.findall(filename)[0][1:]
        ctime = datetime.datetime.fromtimestamp(os.path.getctime(os.path.join(path,filename)))
        # format_path = path.replace(' ','\ ')
        weight = os.path.getsize(os.path.join(path, filename))
        _tmp = pil.open(os.path.join(path, filename))
        shape = _tmp.size

        return Image(
            filename=filename,
            code=code,
            number=number,
            extension=extension,
            ctime=ctime,
            weight=weight,
            path=path,
            shape=shape
        )


class ImageChecker:
    def __init__(self):
        # набор ссылок на картинки каждой из папок в списке папок.
        # меняем картинку self.images -> меняется она же для определенной папки в folders из getImages
        self.images = []

    def getImages(self, folders: list[Folder]):
        for folder in folders:
            self.images += folder.files
        return self.images
        
    def _pickNewest(self, idxs: list[int]):
        imgs = [self.images[i] for i in idxs]
        max(imgs, key=lambda i: i.ctime).newest = True

    def checkNewest(self):
        copies = collections.defaultdict(list)
        for i, image in enumerate(self.images):
            name = '{:05}_{}'.format(image.code,image.number) if image.number \
                    else '{:05}'.format(image.code)
            copies[name].append(i)
        copies = copies.values()
        for idxs in copies:
            if len(idxs) > 1:
                # have copies
                self._pickNewest(idxs)
            else:
                # don't have copies. Automaticly newest
                self.images[idxs[0]].newest = True

class FolderHandler:
    def __init__(self):
        self.codeSearcher = re.compile('^\d{5}')
        self.imageNameChecker = re.compile('^\d{{5}}([-_]\d)?\.(?:{})$'.format('|'.join(supported_extensions)))
        self.imageHandler = ImageHandler()

    def createFolder(self, foldername: str, path: str, files: list[str]) -> Folder:
        try:
            code = int(self.codeSearcher.findall(foldername)[0])
        except Exception as e:
            logging.warning(f"Cant get code of folder {foldername} | {path}. Exception:{e}")
            code = None

        images = []
        for filename in files:
            if self.imageNameChecker.match(filename):
                try:
                    images.append(self.imageHandler.createImage(filename, path))
                except Exception as e:
                    logging.warning(f"Broken Image {filename} | {path}")

        return Folder(
            foldername=foldername,
            code=code,
            path=path,
            files=images
        )

class FolderSearcher:
    def __init__(self):
        self.folderhandler = FolderHandler()
        self.folderNameChecker = re.compile("^\d{5}(\D.*)?$")
        self.folders = []

    def _pickClones(self, idxs: list[int]):
        for idx in idxs:
            tmp = idxs.copy()
            tmp.remove(idx)
            self.folders[idx].clones = [self.folders[i] for i in tmp]
        
    def search(self, search_path):
        """ Make queue of Folders() for checking """
        self.folders = []
        clones = collections.defaultdict(list)
        depth_path = '*'
        for root, _, filenames in os.walk(search_path, topdown=True):
            folder = os.path.basename(root)
            path = os.path.abspath(root)
            # print(root,folder,path,sep='\n',end='\n\n')
            if self.folderNameChecker.match(folder):
                if path.startswith(depth_path+os.sep): 
                    logging.warning(f"5-digit code folder inside of another 5-digit code folder. {path}")
                    continue # внутри есть папки с каким-то кодом
                depth_path = path
                self.folders.append(self.folderhandler.createFolder(folder, path, filenames))
                clones[self.folders[-1].code].append(len(self.folders)-1)
        clones = clones.values()
        for idxs in clones:
            if len(idxs) > 1:
                self._pickClones(idxs)
        return self.folders

class ProdSearcher:
    def __init__(self) -> None:
        self.folderhandler = FolderHandler()
        self.images = []
        self.prodFolder = None

    def search(self, search_path):
        """ Make queue of Folders() for checking """
        self.images = []
        abs_search_path = os.path.abspath(search_path)
        for root, folders, filenames in os.walk(search_path, topdown=True):
            if len(folders):
                logging.warning(f"Folders inside of production directory: {folders}")
            folder = os.path.basename(root)
            path = os.path.abspath(root)
            if path.startswith(abs_search_path+os.sep): continue # skip folders inside
            self.prodFolder = self.folderhandler.createFolder(folder, path, filenames)
            # print(root,folder,path,sep='\n',end='\n\n')
        self.images = self.prodFolder.files
        return self.images

class ProdChecker:
    def __init__(self):
        pass

    def check(self, disk_folders: list[Folder], prod_images: list[Image]):
        for folder in disk_folders:
            for prod_image in prod_images:
                if prod_image.code == folder.code:
                    folder.prodfiles.append(prod_image)
            for disk_image in folder.files:
                if folder.code != disk_image.code: 
                    disk_image.wrong_dir = True
                    continue
                if not disk_image.newest: continue
                for prod_image in prod_images:
                    if disk_image == prod_image:
                        disk_image.onprod = True
                        if disk_image.ctime > prod_image.ctime:
                            disk_image.latest = True
                        # if disk_image.weight <= prod_image.weight:
                        #     disk_image.latest = True
                        break
                disk_image.latest = not disk_image.onprod or disk_image.latest

class Converter:
    """ Convert image """
    def __init__(self, extension='jpg', quality=85):
        self.extension = extension
        self.quality = quality

    def convert_image(self, image: Image, save_path: str):
        filename =  '{:05}_{}.{}'.format(image.code, image.number, self.extension) \
                        if image.number else \
                    '{:05}.{}'.format(image.code, self.extension)
        read_path = os.path.join(image.path, image.filename)
        write_path = os.path.join(save_path, filename)

        if os.path.exists(read_path):
            read_path = read_path.replace(' ', '\ ').replace('(', '\(').replace(')', '\)')
            try:
                if image.shape[0] > 1200:
                    os.system(f"convert {read_path} -resize 1200x -quality {self.quality}% {write_path}")
                else:
                    os.system(f"convert {read_path} -quality {self.quality}% {write_path}")
                    logging.warning(f"Image {filename} width less than 1200px")
            except Exception as e:
                logging.error(f"Can't copy image {read_path} to {write_path}")
        else:
            logging.error(f"Image {read_path} disappeared")

class Mover:
    """ Convert and move images to prod """
    def __init__(self, destination_path: str):
        self.destination_path = destination_path
        self.converter = Converter()

    def move(self, images: list[Image]) -> int:
        logging.info(f'Trying to convert and move {len(images)} objects')
        counter = 0
        print("\t\tConverting and optimizing images:")
        for image in tqdm(images):
            # condition of allowing to copy image to prod
            if image.latest:
                image.moved = True
                self.converter.convert_image(image, self.destination_path)
                counter += 1
        logging.info(f'{counter} objects converted and moved to prod-folder')
        return counter

class Reporter:
    def __init__(self, name, path):
        self.name = name
        self.path = path
        self.table_columns = ["Dir name","Path","Exist files","Added files","Wrong files",
                                "Outsiders","Duplicates","Comment","Statistics"]
        self.report_table = None
    
    def _getPropertiesList(self, folder: Folder, property=lambda x: x.filename, objects=lambda x: x.files, key=lambda x: True):
        return [property(obj) for obj in objects(folder) if key(obj)]

    def _checkImagesTypes(self, folder: Folder):
        main_flag = False
        add_flag = False
        for image in folder.files:
            if image.wrong_dir: continue
            main_flag = image.number==None or main_flag
            add_flag = image.number!=None or add_flag
        if main_flag and add_flag:
            return "Есть оба вида фото"
        elif main_flag:
            return "Только главное фото"
        elif add_flag:
            return "Только дополнительное фото"
        return "Нет фото с подходящим именем"

    def report_stats(self, onprod: int, moved: int):
        tmp = ['' for _ in range(self.report_table.shape[0])]
        tmp[0] = [f"{moved} скопировано / {moved+onprod} всего"]
        self.report_table[self.table_columns[8]] = tmp

    def report_folders(self, folders: list[Folder]):
        report_dict = collections.defaultdict(list)
        for folder in folders: 
            report_dict[self.table_columns[0]].append(folder.foldername)    # Dir name
            report_dict[self.table_columns[1]].append(folder.path)      # Path
            report_dict[self.table_columns[2]].append(self._getPropertiesList(folder, objects=lambda x: x.prodfiles))     # Exist files
            report_dict[self.table_columns[3]].append(self._getPropertiesList(folder, key=lambda x: x.moved))   # Added files
            tmp = self._getPropertiesList(folder, key=lambda x: x.wrong_dir)
            report_dict[self.table_columns[4]].append(tmp)      # Wrong files
            report_dict[self.table_columns[5]].append("Есть неправильные картинки" if tmp else "")      # Outsiders
            report_dict[self.table_columns[6]].append(self._getPropertiesList(folder, property=lambda x: x.foldername, objects=lambda x: x.clones))     # Duplicates
            report_dict[self.table_columns[7]].append(self._checkImagesTypes(folder))   # Comment
        self.report_table = pd.DataFrame.from_dict(report_dict)

    def _save2csv(self):
        self.report_table.to_csv(f"{self.path}/{self.name}.csv", index=False)
    def _save2xls(self):
        self.report_table.to_excel(f"{self.path}/{self.name}.xlsx", index=False)
    def save_log(self):
        self._save2csv()
        try:
            self._save2xls()
        except Exception as e:
            logging.warning("Don't save xlsx. {e}")

if __name__ == "__main__":
    if not os.path.exists(logs_dir):
        os.mkdir(logs_dir)
    if not os.path.exists(reports_dir):
        os.mkdir(reports_dir)
        
    dt = datetime.datetime.now().strftime('%d.%m.%Y %H-%M')
    logging.basicConfig(level=logging.INFO, filename=f"{logs_dir}/sreda-{dt}.log", filemode="w",
                format="%(asctime)s %(levelname)s %(message)s")
    logging.info(f"Starting")

    # ========================================================================
    folderSearcher = FolderSearcher()
    imageChecker = ImageChecker()
    prodSearcher = ProdSearcher()
    prodChecker = ProdChecker()
    mover = Mover(prod_server_dir)

    reporter = Reporter(name="report", path=reports_dir)
    # ========================================================================
    tmp_time = time.time()
    folders = folderSearcher.search(yadisk_dir)
    logging.info(f"Getting folders and images from ya.disk takes {(time.time()-tmp_time):.2f} sec")

    tmp_time = time.time()
    disk_images = imageChecker.getImages(folders)
    imageChecker.checkNewest()
    logging.info(f"Checking images from ya.disk takes {(time.time()-tmp_time):.2f} sec")

    tmp_time = time.time()
    prod_images = prodSearcher.search(prod_server_dir)
    files_exist = len(prod_images)
    logging.info(f"Getting images from production directory takes {(time.time()-tmp_time):.2f} sec")

    tmp_time = time.time()
    prodChecker.check(folders, prod_images)
    logging.info(f"Checking images from production directory takes {(time.time()-tmp_time):.2f} sec")

    tmp_time = time.time()
    files_moved = mover.move(images=disk_images)
    logging.info(f"Compression and moving images to production directory takes {(time.time()-tmp_time):.2f} sec")

    logging.info(f"Done. Found {files_exist} files in prod. Moved {files_moved} files to prod.")
    # ========================================================================
    reporter.report_folders(folders)
    reporter.report_stats(files_exist, files_moved)
    reporter.save_log()
