import CloudKit
import Foundation

protocol RecordModifyingContext: RecordModifyingContextProvider {
  var recordsToSave: [CKRecord.ID: CKRecord] { get }
  var recordIDsToDelete: [CKRecord.ID] { get }
}

protocol RecordModifyingContextProvider {
  var name: String { get }
  var savePolicy: CKModifyRecordsOperation.RecordSavePolicy { get }
  
  func modelChangeForUpdatedRecords<T>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges
  func failedToUpdateRecords<T>(recordsSaved: [CKRecord], recordIDsDeleted: [CKRecord.ID]) -> SyncEngine<T>.ModelChanges
}
