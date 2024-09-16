import CloudKit
import Foundation
import os.log

public enum AccountStatus: Equatable {
  case unknown
  case couldNotDetermine
  case available
  case restricted
  case noAccount
}

extension SyncEngine {
  
  // MARK: - Internal
      
  internal func observeAccountStatus() async throws -> AccountStatus {
    try await withCheckedThrowingContinuation { continuation in
      updateAccountStatus { result in
        switch result {
          case .success(let status):
            continuation.resume(returning: status)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }
  
  // MARK: - Private
  
  internal func updateAccountStatus(_ onCompletion: @escaping ((Result<AccountStatus, Error>) -> Void)) {
    logHandler(#function, .debug)
    
    guard let container else {
      self.accountStatus = .noAccount
      
      onCompletion(.success(.noAccount))
      return
    }
    
    container.accountStatus { [weak self] status, error in
      if let error {
        self?.logHandler("Error retriving iCloud account status: \(error.localizedDescription)", .error)
      }
      
      DispatchQueue.main.async {
        let accountStatus: AccountStatus
        switch status {
          case .available:
            accountStatus = .available
          case .couldNotDetermine:
            accountStatus = .couldNotDetermine
          case .noAccount:
            accountStatus = .noAccount
          case .restricted:
            accountStatus = .restricted
          default:
            accountStatus = .unknown
        }
        self?.accountStatus = accountStatus
        
        if let error {
          onCompletion(.failure(error))
        }
        else {
          onCompletion(.success(accountStatus))
        }
      }
    }
  }
}
