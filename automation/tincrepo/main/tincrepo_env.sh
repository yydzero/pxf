# if TINCHOME not set fail

if [ -z $TINCHOME ] ; then
   echo "TINCHOME not set"
   return 1
fi

export TINCREPOHOME=`pwd`
export PYTHONPATH=$PYTHONPATH:$TINCREPOHOME
export PATH=~/perl5/perlbrew/perls/perl-5.22.4/bin:$PATH
