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
	
def getnext(infile):
	tmp = infile.readline().rstrip()
	if tmp == '':
		return None
	else:
		exec ("outtmp = " + tmp)
		return outtmp

	
	
def doreduce(infilename1, infilename2, outfilename):
	infile1 = open (infilename1, 'r')
	infile2 = open (infilename2, 'r')
	outfile = open (outfilename, 'w')
	
	i = getnext(infile1)
	j = getnext(infile2)
	
	while (i != None and j != None):
		if (i[0] < j[0]):
			outfile.write(str(i) + '\n')
			i = getnext(infile1)
		elif(j[0] < i[0]):
			outfile.write(str(j) + '\n')
			j = getnext(infile2)
		else:
			tmp = (i[0], i[1] + j[1])
			outfile.write(str(tmp) + '\n')
			i = getnext(infile1)
			j = getnext(infile2)
	
	while (i != None) :
		outfile.write(str(i) + '\n')
		i = getnext(infile1)
		
	while (j != None) :
		outfile.write(str(j) + '\n')
		j = getnext(infile2)
	
	infile1.close()
	infile2.close()
	outfile.close()
