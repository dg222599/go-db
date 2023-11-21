package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/jcelliott/lumber"
)


const Version = "1.0.0"


type (
	Logger interface{
		Fatal(string,...interface{})
		Error(string,...interface{})
		Warn(string,...interface{})
		Info(string,...interface{})
		Debug(string,...interface{})
		Trace(string,...interface{})

	}

	Driver struct{
		mutex sync.Mutex
		mutexes map[string]*sync.Mutex
		directory string
		log Logger
	}

)

type Options struct{
	Logger
}

func New(directory string,options *Options)(*Driver,error){
	 directory = filepath.Clean(directory)

	 opts:=Options{}

	 if options !=nil{
		opts = *options
	 }

	 if opts.Logger ==nil{
		opts.Logger = lumber.NewConsoleLogger(lumber.INFO)

	 }

	 driver := Driver{
		directory: directory,
		mutexes: make(map[string]*sync.Mutex),
		log:opts.Logger,

	 }

	 if _,err := os.Stat(directory);err==nil{
		opts.Logger.Debug("Using '%s' (database is already present)\n",directory)
		return &driver,nil
	 }

	 opts.Logger.Debug("Creating the Database at '%s'...\n",directory)
	 return &driver,os.Mkdir(directory,0755)

}

func stat(path string)(fi os.FileInfo,err error){
	 if fi,err = os.Stat(path); os.IsNotExist(err){
		 fi,err = os.Stat(path + ".json")
	 }

	 return 
}

func (d *Driver) Write(collection,resource string,v interface{}) error {
 
	   if collection == ""{
		  return fmt.Errorf("Missing Collection Name can not insert/Create the record")
	   }

	   if resource == ""{
		 return fmt.Errorf("Missing the name of the record can not create  new record in the DB!!")
	   }

	   mutex := d.getOrCreateMutex(collection)
	   mutex.Lock()

	   defer mutex.Unlock()

	   directory :=filepath.Join(d.directory,collection)

	   finalPath:= filepath.Join(directory,resource+".json")

	   tempPath  := finalPath + ".tmp"

	   if err:= os.MkdirAll(directory,0755); err!=nil{
			 return err
	   }

	   b,err := json.MarshalIndent(v,"","\t") 
	   if err !=nil{ 
		return err
	   }

	   b = append(b,byte('\n'))

	   if err := os.WriteFile(tempPath,b,0644); err!=nil{
		 return err
	   }

	   return os.Rename(tempPath,finalPath)
}

func (d *Driver) Delete(collection ,resource string) error {
	 
	    folderName := filepath.Join(collection,resource)
		mutex := d.getOrCreateMutex(collection)
		mutex.Lock()

		defer mutex.Unlock()

		compPath := filepath.Join(d.directory,folderName)

		switch fi,err := stat(compPath);{
		case fi==nil,err!=nil:
				 return fmt.Errorf("unable to find the record/collection")
		case fi.Mode().IsDir():
			return os.RemoveAll(compPath)
		case fi.Mode().IsRegular():
			return  os.RemoveAll(compPath + ".json")
			
		}
		return nil


}

func (d *Driver) Read(collection,resource string,v interface{}) error{
 
	   if collection == ""{
	     return fmt.Errorf("No DB provieded where the record needs to be read!!")
	   }

	   if resource == ""{
		 return fmt.Errorf("No record name provided which needs to be read!!")
	   }

	   record := filepath.Join(d.directory,collection,resource)

	   if _,err := stat(record); err!=nil{
		    return fmt.Errorf("The record does not exist!!")
	   }

	   b,err := os.ReadFile(record + ".json")
	   

	   if err!=nil{
		 return err 
	   }

	   return json.Unmarshal(b,&v)


	  
}

func (d *Driver) ReadAll(collection string) ([]string,error){
		if collection == ""{
			 return nil,fmt.Errorf("Collection not present!!")
		}

		folderName := filepath.Join(d.directory,collection)

		if _,err := stat(folderName); err!=nil{
			 
			return nil,err
		}

		files,_ := os.ReadDir(folderName); 

		var records []string
		
		for _,file := range files{
			b,err := os.ReadFile(filepath.Join(folderName,file.Name())) ; if err!=nil{
				 return nil,err
			}

			records = append(records,string(b))
		}



		return records,nil
}

func (d *Driver) getOrCreateMutex(collection string) *sync.Mutex{

	d.mutex.Lock()
	defer d.mutex.Unlock()
	m,ok := d.mutexes[collection]

	if !ok{
		m  = &sync.Mutex{}
		d.mutexes[collection]  = m
	}

	return m
	  
}

type Address struct{
	 City string
	 State string
	 Country string
	 Pincode json.Number
}

type User struct{
	 Name string
	 Age json.Number
	 Address Address
	 Company string
	 Contact string
}

func main(){
	 
	directory := "./"

	db,err := New(directory,nil); if err!=nil{
		 fmt.Println("Error",err)
	}

	employees :=[]User{

		  {"Max","26",Address{"Spa","North County","Spain","228967"},"Myrl Tech","987889"},
		  {"Checo","33",Address{"Mexico","North County","Spain","228967"},"Myrl Tech","987889"},
		  {"Charles","27",Address{"Monaco","North County","Spain","228967"},"Myrl Tech","987889"},
		  {"Carlos","29",Address{"Ibiza","North County","Spain","228967"},"Myrl Tech","987889"},
		  {"Lewis","37",Address{"Stevenage","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"George","28",Address{"London","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Lando","25",Address{"Southhampton","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Oscar","21",Address{"Melbourne","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Alonso","40",Address{"Madrid","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Stroll","28",Address{"Vancouver","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Daniel","30",Address{"Sydney","North County","Spain","28967"},"Myrl Tech","987889"},
		  {"Alex","26",Address{"Bangkok","North County","Spain","28967"},"Myrl Tech","987889"},
	}

	for _,value := range employees{
		 db.Write("users",value.Name,User{
			Name: value.Name,
			Age: value.Age,
			Address: value.Address,
			Contact: value.Contact,
			Company: value.Company,

		 })
	}

	
	records,err := db.ReadAll("users"); if err !=nil{
		fmt.Println("Error",err)

	}
	fmt.Println(records)
	

	allusers := []User{}

	for _,f := range records{
		employeeFound:=User{}
		if err:=json.Unmarshal([]byte(f),&employeeFound);err !=nil{
			fmt.Println("Error",err)

		}
		allusers = append(allusers,employeeFound)
	}

	fmt.Println(allusers)
	

	// if err := db.Delete("users","Oscar");err!=nil{
	// 		fmt.Println("Error",err)

	// }

	if err:=db.Delete("users",""); err!=nil{
		 fmt.Println("Error",err)
	}



}