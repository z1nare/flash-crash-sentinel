import tkinter as tk
from tkinter import ttk, messagebox
import threading
import time
from datetime import datetime
from collections import deque
import matplotlib.pyplot as plt
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.patches import Rectangle
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import warnings
from services.vol_service import VolService