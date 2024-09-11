from sqlalchemy import create_engine, Column, Float, DateTime, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
import psycopg2
from io import StringIO
from datetime import date, datetime, timedelta
import requests
import time
import numpy as np
import pandas as pd
import yfinance as yf