import numpy as np
import matplotlib.pyplot as plt


def create_histogram(unique_values, all_values, minimum_count=-1):
    
    bins = []
    weights = []
    underflow_bins = []
    underflow_weights = []
    
    for hist_bin in unique_values:
        count = 0
        for value in all_values:
            if value == hist_bin:
                count += 1
        if count > minimum_count:
            bins.append(hist_bin)
            weights.append(count)
        else:
            underflow_bins.append(hist_bin)
            underflow_weights.append(count)
            
    return weights, bins, underflow_weights, underflow_bins


def plot_bar(bins, weights, title='', xlabel='', ylabel='', figsize=(30,10), logy=False):
    
    fig, ax = plt.subplots(1,1,figsize=(30,10))
    ax.set_ylabel(ylabel, size = 24 )
    ax.set_xlabel(xlabel, size = 24 )
    ax.set_title(title, size = 24 ) 
    if logy:
        ax.set_yscale('log')
    ax.bar(bins, weights)
    plt.show()