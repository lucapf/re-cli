from reveal import logging, propertyfinder, pulse



if __name__ == "__main__":
    propertyfinder.get_ads(10) 
    logging.info("propertyfinder - download and process completed")
    filename = pulse.download_transaction()
    # filename =  "transactions_2024-11-13.csv"
    logging.info(f"transactions available in the file {filename}")
    if filename is not None:
        new_items= pulse.load(filename)
        logging.info(f"insert Completed! added {new_items}")
    else:
        logging.info("no data found! skip pulse processing")
    ## change the logic for the save. Use on conflict to nothing and count before and after 



