#!/usr/bin/env python
# coding: utf-8

# # Welcome to Jupyter!

# In[2]:


import pandas


# In[8]:


table = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', header=None, delimiter= ';', nrows = 6)


# In[9]:


table.head()


# In[10]:


table = table.T


# In[11]:


table.head()


# In[3]:


dates = pandas.read_csv('http://webstat.banque-france.fr/fr/downloadFile.do?id=5385698&exportType=csv', header=None, delimiter= ';')


# In[4]:


dates.head(n=10)


# In[5]:


dates = dates.drop([1,2,3,4,5])


# In[6]:


dates.head(10)


# In[14]:


df = pandas.DataFrame(columns=['date','cur'])
for i in dates.columns:
    dates_tmp = dates[dates[i] != '-']
    df['date'] = dates_tmp[0].max()


# In[16]:


df.head(3)


# This repo contains an introduction to [Jupyter](https://jupyter.org) and [IPython](https://ipython.org).
# 
# Outline of some basics:
# 
# * [Notebook Basics](../examples/Notebook/Notebook%20Basics.ipynb)
# * [IPython - beyond plain python](../examples/IPython%20Kernel/Beyond%20Plain%20Python.ipynb)
# * [Markdown Cells](../examples/Notebook/Working%20With%20Markdown%20Cells.ipynb)
# * [Rich Display System](../examples/IPython%20Kernel/Rich%20Output.ipynb)
# * [Custom Display logic](../examples/IPython%20Kernel/Custom%20Display%20Logic.ipynb)
# * [Running a Secure Public Notebook Server](../examples/Notebook/Running%20the%20Notebook%20Server.ipynb#Securing-the-notebook-server)
# * [How Jupyter works](../examples/Notebook/Multiple%20Languages%2C%20Frontends.ipynb) to run code in different languages.

# You can also get this tutorial and run it on your laptop:
# 
#     git clone https://github.com/ipython/ipython-in-depth
# 
# Install IPython and Jupyter:
# 
# with [conda](https://www.anaconda.com/download):
# 
#     conda install ipython jupyter
# 
# with pip:
# 
#     # first, always upgrade pip!
#     pip install --upgrade pip
#     pip install --upgrade ipython jupyter
# 
# Start the notebook in the tutorial directory:
# 
#     cd ipython-in-depth
#     jupyter notebook
