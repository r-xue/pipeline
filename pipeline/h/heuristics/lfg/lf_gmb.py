#!/usr/bin/env python
# coding: utf-8

# In[11]:


#%%timeit -r 1 -n 1
# lf_gma : original code is cppied from example code in wikipedia
## Heavy permutations were used in the wikipedia version.Unnecessary permutations are removed in this version.

# 
## if abs(skew_sp) < 1.0:
##    t_param = 3.0 * sigma_sp   # 1.5
## elif abs(skew_sp) < 2.0:
##    t_param = 2.25 * sigma_sp   # 1.5
## elif abs(skew_sp) < 3.0:
##    t_param = 1.5 * sigma_sp   # 1.5
## else:
##    t_param = (4.5/abs(skew_sp)) * sigma_sp   # 1.5
#
## d_param = len(spectrum)//4     # 2
##
## gm_param = 4 works well

from copy import copy
import numpy as np
import scipy.stats as ss
from numpy.random import default_rng
rng = default_rng()

gm_param = 4
#k_param = 0  ####500  200

loop_better = 1
loop_best = 8 # 8 8st # 8 3
cw0 = 3  # 3     # 2 works well
cwbase = 4 # 4
cwmax = 4  # 4

def_of_weight = 1  # 0: 1 - \rho,  # 1: weight function in m-estimation

regression_dim = 0  # 1: line  or 0: horizontal line

class GM:
    def __init__(self, t=1, d=100, g=4, y_list=[], model=None, loss=None, metric=None): # a little more precise than when d=20
        self.t = t
        self.d = d
        self.g = g
        self.y_list = y_list  
        #self.regression_dim = regression_dim
        self.model = model      # `model`: class implementing `fit` and `predict`
        self.loss = loss        # `loss`: function of `y_true` and `y_pred` that returns a vector
        self.metric = metric    # `metric`: function of `y_true` and `y_pred` and returns a float
        self.best_fit = None
        self.best_error = 10000000.0 #np.inf
        self.best_inliers = []   # for one more fitting
        self.best_d = 0          # Rnaga
        self.gm_weight = None   # Rnaga, May11
        
    def fit(self, X, y, w):
        No_answer = True
        max_inliers_size = 0
        
        gm_param = self.g
        par1 = self.t / gm_param
        par1th = 1.0 / (1/(gm_param**2) + 1)
        
        print('pars',self.t,gm_param,par1,par1th)
        all_ids = np.arange(X.shape[0])
        ones = np.ones((len(all_ids), 1))
        #print('all',all_ids)
        #print('self.y_list',self.y_list)
        
        while (No_answer):
            kmax = len(self.y_list)
            print('kmax,y_list',kmax,self.y_list.T)
            ids = np.arange(kmax)
            idsb = 0
            #done_list = []
            for _ in range(kmax):
                idse = idsb + 1
                maybe_inliers = ids[idsb:idse]
                idsb = idse
                #print('maybe_inliers',maybe_inliers)
                if 0:
                    maybe_model = copy(self.model).fit(X[maybe_inliers], y[maybe_inliers], ones[maybe_inliers])
                else:
                    maybe_model = copy(self.model).fit(X[maybe_inliers], self.y_list[maybe_inliers], ones[maybe_inliers])
                #thresholded = (self.loss(y[all_ids], maybe_model.predict(X[all_ids])) < par1)         ## Rnaga: MSE-version

                gm_dist = self.loss(y[all_ids], maybe_model.predict(X[all_ids]), par1)      ## Rnaga: GM-version 
                
                if def_of_weight:     # w(x)= \rho'(x)/x      # weight function in m-estimation   # works better
                    tmp = ones - gm_dist
                    gm_weight = 2*(tmp * tmp)/(par1*par1)
                else:                 # w(x) = 1 - \rho(x)    # simple version   # this works well, too.
                    gm_weight = ones - gm_dist
    
                thresholded = (gm_dist < par1th)                                ## equivalent to MSE-version
                inlier_ids = all_ids[np.flatnonzero(thresholded).flatten()]   ## Rnaga: inliers_ids are not changed by GM-version
                #print('gm_weight',gm_weight.size)
                #print('size',maybe_model.params[0][0],inlier_ids.size, self.d)
                if inlier_ids.size > max_inliers_size:
                    max_inliers_size = inlier_ids.size
                if inlier_ids.size > self.d:
                    No_answer = False

                    better_model = copy(self.model).fit(X[inlier_ids], y[inlier_ids], gm_weight[inlier_ids])
                    this_error = self.metric(y[all_ids], better_model.predict(X[all_ids]), par1)    ## Rnaga: GM-version
                    
                    for ii in range(loop_better):  # Improvement by walk   # Good effect for def_of_weight = 1.
                        next_gm_dist = self.metric(y[all_ids], better_model.predict(X[all_ids]), par1)                      
                        if def_of_weight:           # w(x)= \rho'(x)/x      # weight function in m-estimation
                            tmp = ones - next_gm_dist
                            next_gm_weight = (2* tmp * tmp)/(par1*par1)
                        else:                                              # w(x) = 1 - \rho(x)    # simple version
                            next_gm_weight = ones - next_gm_dist
                        #next_inlier_ids = all_ids[np.flatnonzero(thresholded).flatten()]   ## Rnaga: inliers_ids are not changed by GM-version
                        param0 = better_model.params[0][0]
                        next_better_model = copy(self.model).fit(X[all_ids], y[all_ids], next_gm_weight[all_ids])
                        dparam0 = next_better_model.params[0][0] - param0
                        next_better_model.params[0][0] = param0 + (cwbase**cw0)*dparam0
                        next_this_error = self.metric(y[all_ids], next_better_model.predict(X[all_ids]), par1)    ## Rnaga: GM-version                    
                        if next_this_error < this_error:
                            #print('next_better',ii+1,next_better_model.params[0][0],next_this_error,this_error)
                            this_error = next_this_error
                            better_model = next_better_model
                            gm_weight = next_gm_weight
                            gm_dist = next_gm_dist
                        else:
                            #print('next_better_model, failed',param0,next_this_error,this_error)
                            break
                    #print('gm_weight',gm_weight.T,total_gm_weight)
                    if this_error < self.best_error:   # mean square error may result in bad estimation
                    #if inlier_ids.size > self.best_d: 
                        self.best_d = inlier_ids.size  # Rnaga
                        self.best_error = this_error
                        self.best_fit = better_model  # Rnaga
                        thresholded = (gm_dist < par1th)
                        inlier_ids = all_ids[np.flatnonzero(thresholded).flatten()]  
                        self.best_inliers = inlier_ids.copy() #for one more fitting
                        self.gm_weight = gm_weight.copy()
                        #print('best_model',self.best_fit.params[0][0],inlier_ids.size,self.best_error)
                        ##print(self.best_d,self.best_error,vars(self.best_fit),self.best_inliers)
                        ##print(vars(self.best_fit))
                
                    #else:
                        #print('next_better_model, pruned', better_model.params[0][0], this_error, self.best_error)
            if No_answer:                              # Rnaga:  Recursive call with y_list modification and self.d reduction
                self.y_list = self.y_list * 2 - self.y_list[len(self.y_list)//2]
                print ('y_list modified')
                self.d = max_inliers_size * 0.8
                print ('** self.d reduction **',self.d)
            #print(self.best_d,self.best_error,vars(self.best_fit),self.best_inliers)
            #print('done_list:',len(done_list))
            
        if not No_answer:
            this_error = self.best_error
            #cwmax = 5
            #dmax = X.shape[0]
            #next_gm_dist = np.ma.arange(cwmax*dmax,dtype=float).reshape(cwmax,dmax)
            for ii in range(loop_best):  # Improvement by walk   # Good effect for def_of_weight = 1.
                next_gm_dist = self.loss(y[all_ids], self.best_fit.predict(X[all_ids]), par1)                      
                if def_of_weight:           # w(x)= \rho'(x)/x      # weight function in m-estimation
                    tmp = ones - next_gm_dist
                    next_gm_weight = (2* tmp * tmp)/(par1*par1)
                else:                                              # w(x) = 1 - \rho(x)    # simple version
                    next_gm_weight = ones - next_gm_dist
                #next_inlier_ids = all_ids[np.flatnonzero(thresholded).flatten()]   ## Rnaga: inliers_ids are not changed by GM-version
                next_better_model = copy(self.model).fit(X[all_ids], y[all_ids], next_gm_weight[all_ids])
                param0 = self.best_fit.params[0][0]
                dparam0 = next_better_model.params[0][0] - param0
                renewal_flag = False
                cwlist = []
                if ii == 0:
                    for cw in range(cwmax):
                        cwlist.append(cwmax-1-cw)
                else:
                    for cw in range(cwmax-1):
                        cwlist.append(cwmax-2-cw)
                    
                for cw in cwlist:
                    next_better_model.params[0][0] = param0 + (cwbase**cw)*dparam0
                    next_this_error = self.metric(y[all_ids], next_better_model.predict(X[all_ids]), par1)    ## Rnaga: GM-version                    
                    #print('maybe_model',ii+1,cw,next_better_model.params[0][0],next_this_error,this_error,self.best_fit.params[0][0])
                    if next_this_error < this_error:
                        renewal_flag = True
                        this_error = next_this_error
                        self.best_fit.params[0][0] = next_better_model.params[0][0]
                        gm_weight = next_gm_weight
                        gm_dist = next_gm_dist
                        #print('updated_model',ii+1,cw,self.best_fit.params[0][0],this_error)
                        break
                if not renewal_flag:
                    break
        ##print(self.best_d,self.best_error,vars(self.best_fit))
        if self.best_fit != None:
            print('*:',self.best_fit.params[0][0],self.best_fit.params[0][0]/self.t)
        return self

    def predict(self, X):
        return self.best_fit.predict(X)

'''def square_error_loss(y_true, y_pred):
    sqe = (y_true - y_pred) ** 2
    #gme = sqe / (self.t + sqe)    # 1.0    0.1
    #return (y_true - y_pred) ** 2
    return sqe

def mean_square_error(y_true, y_pred):
    return np.sum(square_error_loss(y_true, y_pred)) / y_true.shape[0]
'''
def gm_error_loss(y_true, y_pred, c):            ## Rnaga: GM-version
    sqe = (y_true - y_pred) ** 2
    sqc = c * c
    gme = sqe / (sqc + sqe)    # 1.0    0.1
    #return (y_true - y_pred) ** 2
    return gme

def mean_gm_error(y_true, y_pred, c):          ## Rnaga: GM-version
    return np.sum(gm_error_loss(y_true, y_pred, c)) / y_true.shape[0]

class LinearRegressor():
    def __init__(self):
        self.params = None
        self.regression_dim = 1

    def fit(self, X: np.ndarray, y: np.ndarray):
        r, _ = X.shape
        if self.regression_dim > 0:
            X = np.ma.hstack([np.ones((r, 1)), X])
            #print (X.shape, np.ma.dot(X.T,X))
        else:
            X = np.ones((r, 1))           # modified for horizinal base line
        #self.params = np.linalg.inv(X.T @ X) @ X.T @ y                               # @ cannot work with masked array
        #self.params = np.ma.dot(np.linalg.inv(np.ma.dot(X.T,X)),np.ma.dot(X.T,y))     # ma.dot works with masked array
        self.params = np.ma.dot(np.linalg.pinv(X),y)     # ma.dot works with masked array
        return self

    def predict(self, X: np.ndarray):
        r, _ = X.shape
        if self.regression_dim > 0:
            X = np.hstack([np.ones((r, 1)), X])
        else:
            X = np.ones((r, 1))           # modified for horizinal base line
        #return X @ self.params                              # @ cannot work with masked array
        return np.ma.dot(X,self.params)                     # ma.dot works with masked array

class LinearRegressor0():
    def __init__(self):
        self.params = None
        self.regression_dim = 0

    def fit(self, X: np.ndarray, y: np.ndarray, w: np.ndarray):  # Rnaga
        '''r, _ = X.shape
        if self.regression_dim > 0:
            X = np.ma.hstack([np.ones((r, 1)), X])
            #print (X.shape, np.ma.dot(X.T,X))
        else:
            X = np.ones((r, 1))           # modified for horizinal base line
        #self.params = np.linalg.inv(X.T @ X) @ X.T @ y                               # @ cannot work with masked array
        #self.params = np.ma.dot(np.linalg.inv(np.ma.dot(X.T,X)),np.ma.dot(X.T,y))     # ma.dot works with masked array
        self.params = np.ma.dot(np.linalg.pinv(X),y)     # ma.dot works with masked array
        '''
        self.params = np.ma.dot(y.T,w)/np.ma.sum(w)     # Rnaga
        #self.params = np.ma.mean(y)     # ma.dot works with masked array
        return self

    def predict(self, X: np.ndarray):
        r, _ = X.shape
        if self.regression_dim > 0:
            X = np.hstack([np.ones((r, 1)), X])
        else:
            X = np.ones((r, 1))           # modified for horizinal base line
        ##return X @ self.params                              # @ cannot work with masked array
        #return np.ma.dot(X,self.params)                     # ma.dot works with masked array
        return X * self.params 
    
def gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [1 / (sigma * sqrt(2*pi)) *exp(-float(x)**2/(2*sigma**2)) for x in r]
def derivative_of_gaussian(n=11,sigma=1):
    r = range(-int(n/2),int(n/2)+1)
    return [-x / (sigma**3 * sqrt(2*pi)) *exp(-float(x)**2/(2*sigma**2)) for x in r]
def gaussian_average(_spectrum,sigma):
    k_edge = int(sigma * 3)
    ksize= 2 * k_edge + 1
    _g = np.array(gaussian(ksize,sigma))      # normalization
    _g_total = np.sum(_g)
    _g /= _g_total
    #print(np.sum(_g))
    #print('g_a',_spectrum.shape,_g.shape)
    return(np.ma.convolve(_spectrum, _g, propagate_mask=False)[k_edge:-k_edge])  # propagate_mask = False version
def dG(_spectrum,sigma):
    k_edge = int(sigma * 3)
    ksize= 2 * k_edge + 1
    _dg = np.array(derivative_of_gaussian(ksize,sigma))      # normalizationl
    #print(np.sum(_g))
    #print('g_a',_spectrum.shape,_g.shape)
    return(np.ma.convolve(_spectrum, _dg, propagate_mask=False)[k_edge:-k_edge])  # propagate_mask = False version
    
if __name__ == "__main__":
    Database_selector = 1
    _bl_selector = True
    if Database_selector == 0 or Database_selector == 1:
        get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\line-forest')
    elif Database_selector == 2:
        get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\multi-wide-line')
    elif Database_selector == 3:
        get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\weak-wide-line')
    elif Database_selector == 4:
        get_ipython().run_line_magic('cd', 'C:\\Users\\TakeshiShakunaga\\wide-line-at-spw-edge')
    #%cd C:\Users\TakeshiShakunaga\dset_data
    import matplotlib.pyplot as plt
    import scipy.stats as ss
    from math import pi, sqrt, exp, log
    #gm_param = 4
    sigma_pre = 0    
    Draw_graph = True
    loc_all = 1
    if Database_selector == 0:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field1','simplegrid_stage11_spw19_field1',
                              'simplegrid_stage11_spw21_field1','simplegrid_stage11_spw23_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field1','simplegrid_stage9_spw19_field1',
                                    'simplegrid_stage9_spw21_field1','simplegrid_stage9_spw23_field1']
    elif Database_selector == 1:
        if _bl_selector:
            file_name_list_original =['simplegrid_stage11_spw25_field1','simplegrid_stage11_spw27_field1',
                          'simplegrid_stage11_spw29_field1','simplegrid_stage11_spw31_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw25_field1','simplegrid_stage9_spw27_field1',
                                    'simplegrid_stage9_spw29_field1','simplegrid_stage9_spw31_field1']
    elif Database_selector == 2:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field2','simplegrid_stage11_spw19_field2',
                                      'simplegrid_stage11_spw21_field2','simplegrid_stage11_spw23_field2']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field2','simplegrid_stage9_spw19_field2',
                                    'simplegrid_stage9_spw21_field2','simplegrid_stage9_spw23_field2']
    elif Database_selector >= 3:
        if _bl_selector:
            file_name_list_original = ['simplegrid_stage11_spw17_field1','simplegrid_stage11_spw19_field1',
                                      'simplegrid_stage11_spw21_field1','simplegrid_stage11_spw23_field1']
        else:
            file_name_list_original = ['simplegrid_stage9_spw17_field1','simplegrid_stage9_spw19_field1',
                                    'simplegrid_stage9_spw21_field1','simplegrid_stage9_spw23_field1']
    file_name_list = file_name_list_original
    if 1:                              # if 1, select one spw field
        file_name_list = []
        file_name_list.append(file_name_list_original[1])
        loc_all = 0                   # if 0, select locations by [loc_b loc_e]
        if loc_all == 0:
            loc_b =  99 #139 #72
            if Database_selector == 3:
                loc_b = 102
            elif Database_selector == 4:
                loc_b = 0
            loc_e = loc_b + 9 #30  #15
    for file_name in file_name_list:
        data_file =file_name+'.npz'
        data = np.load(data_file)                         # np.load('data1_17_1.npz')
        #data = np.load(directory_name+data_file)         # np.load('data1_17_1.npz')
        full_spectrum = data['data'][:,:]
        fs_mask = data['mask'][:,:]
        fs = np.ma.array(full_spectrum,mask=fs_mask)
        #print(fs_mask,np.sum(fs_mask))
        print(fs.shape)

        spectrum = np.ma.arange(fs.shape[1], dtype=np.float64)

        loc_max = fs.shape[0]
        if loc_all:
            loc_b = 0 #loc_max // 4 #loc_max//4 #//4
            loc_e = loc_max #loc_b + 3 #loc_max // 2 #loc_max // 2 #int(loc_max//2) #loc_max//2 #loc_b + 1
        print('loc_b,loc_e',loc_b,loc_e,loc_max,fs.shape[1])
        base_l = np.arange(loc_e-loc_b, dtype=np.float64)
        base_lr = np.arange(loc_e-loc_b, dtype=np.float64)
        color = ["red", "yellow", "green", "blue", "purple"]
        for loc in range(loc_b,loc_e):
            str1 = format(loc,'03d')
            data_file = file_name + '.npz#' + str1
            #spectrum = fs[loc,:]
            for j in range(spectrum.shape[0]):
                spectrum[j] = fs[loc,j]

            X = np.ma.arange(len(spectrum)).reshape(-1,1)   # ma doesn't work with @
            if Draw_graph:
                plt.style.use("seaborn-darkgrid")
                plt.rcParams["figure.figsize"] = [20,6]#ax.set_box_aspect(0.5)  # 0.2     1
                parameters = {'axes.labelsize': 25,'axes.titlesize': 35,'xtick.labelsize':20,'ytick.labelsize':20}
                plt.rcParams.update(parameters)
                fig, ax = plt.subplots(1, 1)
            ii = -1

            m_param=1 # 10, 100
            thr_rmg = 2.25
            e_std_max = 12
            for m in range(m_param):      # if 1:
                mean_sp = np.ma.mean(spectrum)
                height_sp = np.ma.max(spectrum)-mean_sp
                depth_sp = mean_sp - np.ma.min(spectrum)
                sigma_sp = np.ma.std(spectrum)
                mean_level = np.arange(e_std_max,dtype=float)
                cut_level = np.arange(e_std_max,dtype=float)
                #skew_sp = ss.skew(spectrum)
                spectrum1 = spectrum
                for e_std in range(e_std_max):
                    if height_sp > depth_sp:
                        mean_level[e_std]=mean_sp
                        cut_level[e_std]=mean_sp+sigma_sp*2
                        upper_part = np.where(spectrum1>cut_level[e_std])
                        if e_std == 0:                              # For initial division
                            thr_level = 2.0
                            while upper_part[0].shape[0] == 0:      # Very rare cases, thr_level is tuned for the set
                                thr_level -= 0.33
                                cut_level[e_std]=mean_sp+sigma_sp*thr_level
                                upper_part = np.where(spectrum1>cut_level[e_std])
                        lower_part = np.where(spectrum1<=cut_level[e_std])
                    else:
                        mean_level[e_std]=mean_sp
                        cut_level[e_std]=mean_sp-sigma_sp*2
                        lower_part = np.where(spectrum1<cut_level[e_std])
                        if e_std == 0:                              # For initial division
                            thr_level = 2.0
                            while lower_part[0].shape[0] == 0:      # Very rare cases, thr_level is tuned for the set
                                thr_level -= 0.33
                                cut_level[e_std]=mean_sp-sigma_sp*thr_level
                                lower_part = np.where(spectrum1<cut_level[e_std])
                        upper_part = np.where(spectrum1>=cut_level[e_std])
                    upper_mean = np.ma.mean(spectrum1[upper_part])
                    lower_mean = np.ma.mean(spectrum1[lower_part])
                    mean_gap = upper_mean - lower_mean
                    relative_mean_gap = mean_gap/sigma_sp
                    print('statistics0:',e_std,mean_gap,sigma_sp,relative_mean_gap,mean_level[e_std],cut_level[e_std])
                    if height_sp > depth_sp:
                        sigma_sp = np.ma.std(spectrum1[lower_part])
                        #skew_sp = ss.skew(spectrum1[lower_part])
                        mean_sp = lower_mean
                    else:
                        #print('**negative')
                        sigma_sp = np.ma.std(spectrum1[upper_part])
                        #skew_sp = ss.skew(spectrum1[upper_part])
                        mean_sp = upper_mean

                    if relative_mean_gap > thr_rmg:           # thr_tmg = 2.25 is empirically tuned.   # relative_mean_gap converges to 2.25
                        if height_sp > depth_sp:
                            spectrum1 = spectrum1[lower_part]             # This substitution is important for convergence 
                        else:
                            spectrum1 = spectrum1[upper_part]             # This substitution is important for convergence 
                        #print('statistics1:',e_std,mean_gap,sigma_sp,relative_mean_gap)
                    else:
                        mean_level[e_std+1]=mean_sp
                        break
                print('sigma_sp,mean_sp',sigma_sp,mean_sp)

                ##spectrum_ = spectrum.reshape(-1,1)
                #print(spectrum.shape,spectrum_.shape)
                #regressor =lfr.GM(model=lfr.LinearRegressor(), loss=lfr.square_error_loss, metric=lfr.mean_square_error)
                # Important parameters for lfr.GM()
                n_param = 1
                k_param = 0      #k_param = 200  ####500  200
                t_param = 3.0 * sigma_sp
                d_param = len(spectrum)//4     # 2

                if sigma_pre == 0:
                    y = np.ma.array(spectrum).reshape(-1,1) 
                else:            ##[]
                    y = gaussian_average(spectrum, sigma_pre).reshape(-1,1) 
                if k_param == 0:
                    ycmax = 1
                    if ycmax == 1:
                        ycgap = 0
                    else:
                        ycgap = sigma_sp*2/(ycmax-1)
                    yc_list = np.ma.array(copy(spectrum[0:ycmax])).reshape(-1,1)
                    for yc in range(ycmax):
                        yc_list[yc]= mean_sp + ycgap* (yc - ycmax//2)
                #data_length, _ = X.shape

                if regression_dim == 1:           ## Rnaga: GM-version
                    #regressor = RANSAC(n=3,k=1000,t=t_param,d=data_length//1.25, model=LinearRegressor(), loss=square_error_loss, metric=mean_square_error)
                    regressor =GM(t=t_param, g=gm_param, y_list=yc_list, model=LinearRegressor(), loss=gm_error_loss, metric=mean_gm_error)
                else:                             ## Rnaga: GM-version
                    #regressor = RANSAC(n=n_param,k=k_param,t=t_param,d=d_param, model=LinearRegressor0(), loss=square_error_loss, metric=mean_square_error)
                    regressor = GM(t=t_param,g=gm_param, y_list=yc_list, model=LinearRegressor0(), loss=gm_error_loss, metric=mean_gm_error)
                ones = np.ones((len(X), 1))
                a=regressor.fit(X, y, ones)
                #print(vars(a.best_fit))

                if Draw_graph:
                    line = np.linspace(min(X), max(X), num=100).reshape(-1, 1)
                    #print (line, regressor.predict(line))
                    #plt.plot(line, regressor.predict(line), c="red")
                    if m==0:
                        plt.scatter(X,np.ma.array(spectrum).reshape(-1,1),color="peru")
                        plt.scatter(X, y)
                    else:
                        plt.scatter(X,np.ma.array(spectrum).reshape(-1,1))
                    plt.plot(line, regressor.predict(line), c=color[m % 5])
            #plt.show()
            if Draw_graph:
                str0 = format(n_param,'01d') 
                str1 = format(gm_param,'01d')
                str2 = format(k_param,'01d')
                str2a = format(ycmax,'01d')
                if loop_better == 0:
                    str3a = format(loop_better,'01d')
                else:
                    str3a = format(cw0,'01d')
                str3b = format(loop_best,'01d')
                str4a = format(cwmax,'01d')
                str4b = format(cwbase,'01d')
                str5 = format(t_param,'3.2f')
                stra = format(def_of_weight,'01d')
                
                #strb = format(Burst_noise_reduction,'01d')
                if m_param == 1000:
                    resultfile='result_gm_'+file_name+'_'+str1+'_'+str3a+str3b+'('+str4b+str4a+'_'+')_'+stra+'_'+str2+'('+str2a+')_'+str5+'.png'
                else:
                    resultfile='result_gm_'+file_name+'_'+str1+'_'+str3a+str3b+'('+str4b+str4a+'_'+')_'+stra+'_'+str2+'('+str2a+')_'+str5+'.png'
                plt.savefig(resultfile)


# In[2]:


range(2,5)
list(range(2,5))


# In[3]:


a = 7 % 5
print (a)


# In[4]:


import numpy as np
y = np.arange(10)
y = y * 2
ind = [2]
y[ind][0]


# In[ ]:





# In[ ]:





# In[ ]:




