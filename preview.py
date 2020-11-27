import matplotlib.pyplot as plt
import os
from pylab import rcParams
    
def preview(plot=None):
    rcParams['figure.figsize'] = 5,5

    labels = [name for name in os.listdir('.') if os.path.isdir(name)]
    sizes = []

    for db in labels:
        total_size = 0
        start_path = db
        for path, dirs, files in os.walk(start_path):
            for f in files:
                fp = os.path.join(path, f)
                total_size += os.path.getsize(fp)
        sizes.append(total_size)

    plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, pctdistance=0.85)

    plt.title('Total DB Size Distribution', fontsize=18)

    total_db = sum(sizes)
    plt.annotate('Total size', xy=(-0.29, -0.0), size=16, color='darkred')
    plt.annotate(str(int(round(total_db/1e6)))+' MB', xy=(-0.18,-0.15), size=15, color='#d62728')

    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

    plt.tight_layout()

    if plot is None:
        plt.show()
    else:
        plt.savefig(plot)
