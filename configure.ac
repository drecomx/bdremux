AC_INIT([bdremux],[0.1],[fraxinas@opendreambox.org])
AM_INIT_AUTOMAKE([foreign])

AC_CONFIG_SRCDIR([src/bdremux.c])

m4_ifdef([AM_SILENT_RULES], [AM_SILENT_RULES([yes])])
AC_USE_SYSTEM_EXTENSIONS
AC_SYS_LARGEFILE

dnl find a compiler
AC_PROG_CC
AC_PROG_CC_STDC

dnl check if the compiler supports '-c' and '-o' options
AM_PROG_CC_C_O

# Checks for header files.
AC_CHECK_HEADERS([stdio.h stdlib.h fcntl.h string.h getopt.h byteswap.h netinet/in.h])

# Checks for typedefs, structures, and compiler characteristics.
AC_TYPE_SIZE_T

PKG_CHECK_MODULES(GST, gstreamer-0.10)

AC_CONFIG_FILES([
Makefile
src/Makefile
])
AC_OUTPUT
