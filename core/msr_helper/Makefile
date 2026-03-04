CC = gcc
CFLAGS = -Wall -O2
TARGET = msr_read
SOURCE = msr_read.c

all: $(TARGET)

$(TARGET): $(SOURCE)
	$(CC) $(CFLAGS) -o $(TARGET) $(SOURCE)

clean:
	rm -f $(TARGET)

install: $(TARGET)
	@echo "Setting capabilities on $(TARGET)..."
	sudo setcap cap_sys_rawio=ep $(TARGET)
	@echo "Done. You can now run ./$(TARGET) without sudo"
	@ls -la $(TARGET)

.PHONY: all clean install
