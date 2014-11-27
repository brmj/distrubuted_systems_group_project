
def filetolist(filename):
	infile = open(filename, 'r')
	outlist = []
	for line in infile:
		exec("tmp = " + line.rstrip())
		outlist.append(tmp)
	infile.close()
	return outlist
	
def listtofile(filename, l):
	outfile = open(filename, 'w')
	for item in l:
		outfile.write(str(item) + '\n')
	outfile.close()
	


def tupleize(l):
	output = []
	for item in l:
		output.append((item,1))
	return output
	
	
def preprocess(filename):
	infile = open(filename, 'r')
	outlist = []
	for line in infile:
		exec("tmp = " + line.rstrip())
		outlist.append((tmp, 1))
	infile.close()
	return outlist
	
def domap(infilename, outfilename):
	tosort = filetolist(infilename)
	output = (sorted((x,tosort.count(x)) for x in set(tosort)))
	listtofile(outfilename, output)
	


def merge(l1, l2):
	output = []
	while len(l1) > 0 and len(l2) > 0:
		if l1[0][0] < l2[0][0]:	
			output.append(l2.pop(0))
		elif l2[0][0] < l1[0][0]:
			output.append(l1.pop(0))
		else:
			l2.append((l1[0][0], l1.pop(0)[1] + l2.pop(0)[1]))
		

	if len(l1) > 0:
		output.extend(l1)
	else:
		output.extend(l2)

	return output

def mergesort(list):
    if len(list) <= 1:
		return list
	
    middle = len(list) // 2
    l1 = list[:middle]
    l2 = list[middle:]
 
    l1 = mergesort(l1)
    l2 = mergesort(l2)
    return merge(l1, l2)
    
    
