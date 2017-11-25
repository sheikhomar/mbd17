import matplotlib.pyplot as plt



data = [(0, 67), (2, 69), (3, 59), (4, 77), (6, 70), (7, 49), (9, 52), (10, 39), (13, 34), (14, 30), (15, 37), (16, 37), (17, 29)]

fig, ax = plt.subplots()
X, Y = zip(*data)
ax.bar(X, Y)
plt.show()
