import torch
import torch.nn as nn
# from torchvision.datasets import ImageNet
from torchvision.models import resnet18, alexnet
from torchvision.transforms import *
from torchvision.io import read_image
from torch.utils.data import DataLoader, Dataset
import torch.nn.functional as F
import os
import numpy as np
from tqdm import tqdm
import time

# class ImageNet(Dataset):
#     def __init__(self, start=0, end=50000, transform=None):
#         self.database = self.query_data(start, end)
#         self.labels = self.query_label(start, end)
#         # self.transform = transform if transform else Resize((112, 112))
#         self.transform = transform if transform else Compose([Resize((224, 224)), Normalize([0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])
#         self.to_RGB = Lambda(lambda x: x.repeat(3, 1, 1))
    
#     def query_data(self, start, end):
#         database = os.listdir('imageNet/val')
#         database.sort()
#         database = database[start:end]
#         database = ['imageNet/val/' + filename for filename in database]
#         return database
    
#     def query_label(self, start, end):
#         with open('imageNet/ILSVRC2012_validation_ground_truth.txt', 'r') as f:
#             content = f.read().split('\n')
#         labels = content[start:end]
#         labels = [int(label) - 1 for label in labels]
#         #convert to tensor and one-hot encoding
#         labels = F.one_hot(torch.tensor(labels), num_classes=1000)
#         return labels

#     def __len__(self):
#         return self.labels.size(0)
    
#     def __getitem__(self, idx):
#         img = read_image(self.database[idx])
#         if img.size(0) == 1:
#             img = self.to_RGB(img)
#         img = self.transform(img.float())
#         # print(img.size())
#         # exit(1)
#         return img, self.labels[idx]

class ImageNet(Dataset):
    def __init__(self, data, labels, transform=None):
        self.database = data
        self.labels = labels
        # self.transform = transform if transform else Resize((112, 112))
        self.transform = transform if transform else Compose([Resize((224, 224)), Normalize([0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225])])
        self.to_RGB = Lambda(lambda x: x.repeat(3, 1, 1))

    def __len__(self):
        return self.labels.size(0)
    
    def __getitem__(self, idx):
        img = self.database[idx]
        if img.size(0) == 1:
            img = self.to_RGB(img)
        img = self.transform(img.float())
        return img, self.labels[idx]

class Model(nn.Module):
    def __init__(self, nn_name):
        super().__init__()
        self.model = self.load_pretrained(nn_name)
        self.testdata = None
        self.testloader = None
    
    def load_data(self, data, labels):
        self.testdata = ImageNet(data, labels)
        self.testloader = DataLoader(self.testdata, batch_size=BATCH_SIZE, worker_num=4, shuffle=True)

    def load_pretrained(self, nn_name):
        '''
        emulate training stage, instead of doing the actual training,
        we load the pretrained model directly and use eval() to disable autograd
        '''
        if nn_name == "resnet18":
            model = resnet18(pretrained=True)
        elif nn_name == "alexnet":
            model = alexnet(pretrained=True)
        else:
            raise NotImplementedError(f"no compatible trained model in task: {nn_name}!")
        model.eval()
        print("training stage finished, ready to do inference!")
        return model
    
    def get_accuracy(self, pred_batches, label_batches):
        # print(pred_batches[0, 488:492], pred_batches[0].max(), pred_batches[0].argmax())
        # print(label_batches.argmax(dim=1))
        # print(pred_batches[:5, :5])
        # print(label_batches[:5, :5])
        # print(pred_batches.size(), label_batches.size())
        # pred_label = F.one_hot(pred_batches.argmax(dim=1), num_classes=1000)
        # print(pred_label.size())
        # TP = (pred_label * label_batches).sum()
        pred_label = pred_batches.argmax(dim=1)
        return pred_label, pred_label.size(0)
    
    def predict(self, dataloader):
        '''
        input: a dataloader that load assigned data in batches
        return: the accuracy in this portion together with the number of assigned data in this task
        '''
        counter = 0
        pred_batches = []
        label_batches = []
        with torch.no_grad():
            for data, label in tqdm(dataloader):
                pred = self.model(data)
                counter += pred.size(0)
                pred_batches.append(pred)
                label_batches.append(label)
        return self.get_accuracy(torch.cat(pred_batches, dim=0), torch.cat(label_batches, dim=0))